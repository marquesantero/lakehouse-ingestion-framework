"""Orquestrador de streaming/Auto Loader."""
from __future__ import annotations

import logging
import traceback
from datetime import datetime
from dataclasses import replace
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame

from ._spark import runtime_info
from ._sql import new_run_id, today_str, utc_now_str, utc_now_ts
from .config import CTRL_SCHEMA_VERSION, FRAMEWORK_VERSION
from .ingestion import _base_result_payload, _short_error_message, ingest_plan
from .plan import IngestionPlan, SourceSpec, target_full_table_name, validate_plan_shape
from .sources import get_source_resolver
from .state import (
    acquire_lock,
    ctrl_table_names,
    ensure_ctrl_tables,
    find_idempotent_stream,
    log_error,
    log_stream_finish,
    log_stream_start,
    release_lock,
    stream_child_run_metrics,
)

logger = logging.getLogger("contractforge")


def _stream_source_name(source: SourceSpec) -> str:
    return f"{source.type}:{source.path}"


def _int_metric(payload: Dict[str, Any], *keys: str) -> int:
    """Lê métricas de payloads antigos/novos sem depender de um único nome."""
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return int(value or 0)
    return 0


def _stream_metrics_from_batches(batch_results: list[Dict[str, Any]]) -> Dict[str, int]:
    """Agrega métricas de micro-batches já retornados por ``ingest_plan``."""
    return {
        "batches_processed": len(batch_results),
        "total_rows_read": sum(
            _int_metric(result, "rows_read", "total_rows_read") for result in batch_results
        ),
        "total_rows_written": sum(
            _int_metric(result, "rows_written", "total_rows_written") for result in batch_results
        ),
        "total_rows_quarantined": sum(
            _int_metric(result, "rows_quarantined", "total_rows_quarantined")
            for result in batch_results
        ),
    }


def _prefer_child_stream_metrics(local: Dict[str, int], child: Dict[str, int]) -> bool:
    """Escolhe métricas persistidas quando elas são mais completas que as locais."""
    if child["batches_processed"] <= 0:
        return False
    local_rows = (
        local["total_rows_read"]
        + local["total_rows_written"]
        + local["total_rows_quarantined"]
    )
    child_rows = (
        child["total_rows_read"]
        + child["total_rows_written"]
        + child["total_rows_quarantined"]
    )
    return (
        local["batches_processed"] == 0
        or child["batches_processed"] > local["batches_processed"]
        or child_rows > local_rows
    )


def _stream_result(
    plan: IngestionPlan,
    stream_run_id: str,
    target: str,
    source_name: str,
    status: str,
    started_dt: datetime,
    runtime_meta: Dict[str, Optional[str]],
    batch_results: list[Dict[str, Any]],
    stage_durations: Dict[str, float],
    error: Optional[str] = None,
    skip_reason: Optional[str] = None,
    skipped_by_stream_run_id: Optional[str] = None,
    stream_metrics: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    finished_dt = utc_now_ts()
    metrics = stream_metrics or _stream_metrics_from_batches(batch_results)
    return {
        **_base_result_payload(
            status,
            plan,
            target,
            source_name,
            runtime_meta,
            stream_run_id=stream_run_id,
        ),
        "batches_processed": metrics["batches_processed"],
        "total_rows_read": metrics["total_rows_read"],
        "total_rows_written": metrics["total_rows_written"],
        "total_rows_quarantined": metrics["total_rows_quarantined"],
        "batch_results": batch_results,
        "stage_durations": stage_durations,
        "duration_seconds": (finished_dt - started_dt).total_seconds(),
        "error_message": _short_error_message(error),
        "skip_reason": skip_reason,
        "skipped_by_stream_run_id": skipped_by_stream_run_id,
    }


def _stream_start_payload(
    plan: IngestionPlan,
    stream_run_id: str,
    target: str,
    started_dt: datetime,
    runtime_meta: Dict[str, Optional[str]],
    status: str,
) -> Dict[str, Any]:
    return {
        "stream_run_id": stream_run_id,
        "idempotency_key": plan.idempotency_key,
        "idempotency_policy": plan.idempotency_policy,
        "target_table": target,
        "target_catalog": plan.catalog,
        "target_layer": plan.layer,
        "notebook_name": plan.notebook_name,
        "source_type": plan.source.type,
        "source_path": plan.source.path,
        "trigger": plan.source.trigger,
        "checkpoint_location": plan.source.checkpoint_location,
        "status": status,
        "started_at_utc": started_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "batches_processed": 0,
        "total_rows_read": 0,
        "total_rows_written": 0,
        "total_rows_quarantined": 0,
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        **runtime_meta,
        "master_job_id": plan.master_job_id,
        "master_run_id": plan.master_run_id,
        "parent_run_id": plan.parent_run_id,
        "run_group_id": plan.run_group_id,
    }


def ingest_stream_plan(plan: IngestionPlan) -> Dict[str, Any]:
    """Executa ``SourceSpec`` em Autoloader ``available_now``.

    Cada micro-batch vira uma chamada a ``ingest_plan`` com ``source=batch_df``.
    A execução externa registra o ciclo em ``ctrl_ingestion_streams``.
    """
    validate_plan_shape(plan)
    if not isinstance(plan.source, SourceSpec):
        raise ValueError("ingest_stream_plan requer plan.source como SourceSpec")

    stream_run_id = new_run_id()
    run_date = today_str()
    started_dt = utc_now_ts()
    target = target_full_table_name(plan)
    source_name = _stream_source_name(plan.source)
    runtime_meta = runtime_info()
    stage_durations: Dict[str, float] = {}
    batch_results: list[Dict[str, Any]] = []
    stream_metrics: Optional[Dict[str, int]] = None
    status = "SUCCESS"
    error: Optional[str] = None
    error_type: Optional[str] = None
    skip_reason: Optional[str] = None
    skipped_by_stream_run_id: Optional[str] = None
    tables = ctrl_table_names(plan.catalog, plan.ctrl_schema) if plan.dry_run else None
    lock_acquired = False
    stream_logged = False

    if plan.dry_run:
        return _stream_result(
            plan,
            stream_run_id,
            target,
            source_name,
            "DRY_RUN",
            started_dt,
            runtime_meta,
            batch_results,
            stage_durations,
        )

    try:
        stage_started = utc_now_ts()
        tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
        stage_durations["control_setup"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        previous_success = find_idempotent_stream(
            tables, target, plan.idempotency_key, status="SUCCESS"
        )
        stage_durations["idempotency"] = (utc_now_ts() - stage_started).total_seconds()
        previous_status = previous_success.get("status") if previous_success else None
        previous_stream_run_id = previous_success.get("stream_run_id") if previous_success else None

        if (
            plan.idempotency_policy in {"skip_if_success", "rerun_if_failed"}
            and previous_status == "SUCCESS"
        ):
            status = "SKIPPED"
            skip_reason = "idempotency_key_already_succeeded"
            skipped_by_stream_run_id = previous_stream_run_id
            payload = _stream_start_payload(plan, stream_run_id, target, started_dt, runtime_meta, status)
            payload.update(
                {
                    "skip_reason": skip_reason,
                    "skipped_by_stream_run_id": skipped_by_stream_run_id,
                    "ended_at_utc": utc_now_str(),
                    "duration_seconds": (utc_now_ts() - started_dt).total_seconds(),
                }
            )
            log_stream_start(tables, payload)
            stream_logged = True
            return _stream_result(
                plan,
                stream_run_id,
                target,
                source_name,
                status,
                started_dt,
                runtime_meta,
                batch_results,
                stage_durations,
                skip_reason=skip_reason,
                skipped_by_stream_run_id=skipped_by_stream_run_id,
            )

        if plan.idempotency_policy == "fail_if_success" and previous_status == "SUCCESS":
            raise RuntimeError(
                "idempotency_policy=fail_if_success bloqueou o stream: "
                f"idempotency_key={plan.idempotency_key!r} já teve sucesso em stream_run_id={previous_stream_run_id}"
            )

        log_stream_start(tables, _stream_start_payload(plan, stream_run_id, target, started_dt, runtime_meta, "RUNNING"))
        stream_logged = True

        if plan.lock_enabled:
            stage_started = utc_now_ts()
            acquire_lock(tables, target, stream_run_id, owner=stream_run_id)
            lock_acquired = True
            stage_durations["lock_acquire"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        stream_df, source_name = get_source_resolver(plan.source.type).resolve_stream(plan.source, plan)
        stage_durations["stream_resolve"] = (utc_now_ts() - stage_started).total_seconds()

        def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_key_prefix = plan.idempotency_key or stream_run_id
            sub_plan = replace(
                plan,
                source=batch_df,
                parent_run_id=stream_run_id,
                lock_enabled=False,
                idempotency_key=f"{batch_key_prefix}:batch:{batch_id}",
                idempotency_policy="skip_if_success",
            )
            result = ingest_plan(sub_plan)
            batch_results.append(result)
            if result["status"] == "FAILED":
                raise RuntimeError(
                    f"Batch {batch_id} falhou em stream_run_id={stream_run_id}: "
                    f"{result.get('error_message')}"
                )

        stage_started = utc_now_ts()
        query = (
            stream_df.writeStream.foreachBatch(_process_batch)
            .option("checkpointLocation", plan.source.checkpoint_location)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        stage_durations["stream_run"] = (utc_now_ts() - stage_started).total_seconds()

    except Exception as exc:
        status = "FAILED"
        error_type = type(exc).__name__
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error("Stream de ingestão falhou: %s", exc)
    finally:
        finished_dt = utc_now_ts()
        if tables and lock_acquired:
            release_lock(tables, target, stream_run_id)
        if tables:
            stream_metrics = _stream_metrics_from_batches(batch_results)
            if status != "SKIPPED":
                child_metrics = stream_child_run_metrics(tables, stream_run_id)
                if _prefer_child_stream_metrics(stream_metrics, child_metrics):
                    stream_metrics = child_metrics
            if not stream_logged:
                try:
                    log_stream_start(
                        tables,
                        _stream_start_payload(plan, stream_run_id, target, started_dt, runtime_meta, status),
                    )
                    stream_logged = True
                except Exception as start_log_exc:
                    logger.error("Falha ao registrar início do stream: %s", start_log_exc)
            try:
                log_stream_finish(
                    tables,
                    stream_run_id,
                    {
                        "status": status,
                        "ended_at_utc": finished_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration_seconds": (finished_dt - started_dt).total_seconds(),
                        "batches_processed": stream_metrics["batches_processed"],
                        "total_rows_read": stream_metrics["total_rows_read"],
                        "total_rows_written": stream_metrics["total_rows_written"],
                        "total_rows_quarantined": stream_metrics["total_rows_quarantined"],
                        "error_message": _short_error_message(error),
                    },
                )
            except Exception as log_exc:
                logger.error("Falha ao registrar stream: %s", log_exc)
            if error:
                try:
                    log_error(
                        tables,
                        {
                            "run_id": stream_run_id,
                            "error_ts_utc": utc_now_str(),
                            "error_date": run_date,
                            "target_table": target,
                            "source_table": source_name,
                            "mode": plan.mode,
                            "status": status,
                            "error_type": error_type,
                            "error_message": _short_error_message(error),
                            "stack_trace": error,
                            "framework_version": FRAMEWORK_VERSION,
                            "ctrl_schema_version": CTRL_SCHEMA_VERSION,
                            **runtime_meta,
                        },
                    )
                except Exception as error_log_exc:
                    logger.error("Falha ao registrar erro completo do stream: %s", error_log_exc)

    return _stream_result(
        plan,
        stream_run_id,
        target,
        source_name,
        status,
        started_dt,
        runtime_meta,
        batch_results,
        stage_durations,
        error=error,
        skip_reason=skip_reason,
        skipped_by_stream_run_id=skipped_by_stream_run_id,
        stream_metrics=stream_metrics,
    )
