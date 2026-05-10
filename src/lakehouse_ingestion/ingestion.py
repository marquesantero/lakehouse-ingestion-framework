"""Orquestrador principal: ``ingest`` e ``ingest_plan``.

Composição mínima — toda a lógica está em submódulos. Aqui fica só o fluxo:
preparar DataFrame, validar, avaliar qualidade, escrever, registrar runs/state,
emitir lineage.
"""
from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import CONFIG, FrameworkConfig  # noqa: F401
from .lineage import capture_explain, write_explain_plan, write_openlineage_event
from .plan import IngestionPlan, QualityRules, build_plan_from_kwargs  # noqa: F401
from .quality import evaluate_quality, is_abort_only_failure, write_quality_results, write_quarantine
from .schema import (
    build_custom_keys,
    deduplicate_by_order,
    fix_encoding,
    sync_delta_schema,
    table_exists,
    validate_schema_policy,
)
from ._spark import safe_cache, safe_unpersist, spark
from ._sql import (
    full_table_name,
    new_run_id,
    safe_truncate,
    to_json,
    today_str,
    utc_now_str,
    utc_now_ts,
    validate_cols,
)
from .state import (
    acquire_lock,
    ctrl_table_names,
    ensure_ctrl_tables,
    log_run,
    release_lock,
    upsert_state,
    with_retry,
)
from .watermark import apply_watermark, compute_watermark, get_watermark
from .writers import (
    affected_partition_values,
    delta_version,
    execute_write_mode,
    extract_row_metrics,
    latest_operation_metrics,
    run_optimize,
    write_strategy,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lakehouse_ingestion")


def _resolve_source(plan: IngestionPlan) -> Tuple[DataFrame, str]:
    """Resolve ``plan.source`` em ``(DataFrame, nome_qualificado_para_log)``.

    String é interpretada como nome de tabela; se não tiver pontos, é
    qualificada com ``catalog.layer.<source>``. DataFrames passam direto e
    recebem o rótulo ``"dataframe"`` no log.
    """
    if isinstance(plan.source, str):
        source_full = (
            plan.source
            if "." in plan.source
            else full_table_name(plan.catalog, plan.layer, plan.source)
        )
        return spark.read.table(source_full), source_full
    return plan.source, "dataframe"


def _prepare_dataframe(
    df: DataFrame,
    plan: IngestionPlan,
    run_id: str,
    run_date: str,
    wm_prev: Optional[str],
) -> DataFrame:
    """Aplica todas as transformações pré-quality em ordem determinística.

    Sequência: ``select_columns`` -> ``filter_expression`` -> ``custom_keys``
    -> ``apply_watermark`` -> ``deduplicate_by_order`` -> ``fix_encoding`` ->
    adição das colunas de controle (``ingestion_date``, ``source_system``,
    ``__run_id``).
    """
    if plan.select_columns:
        validate_cols(df, plan.select_columns, "select_columns")
        df = df.select(*plan.select_columns)
    if plan.filter_expression:
        df = df.where(plan.filter_expression)
    if plan.custom_keys:
        df = build_custom_keys(df, plan.custom_keys)
    if plan.watermark_columns:
        df = apply_watermark(df, plan.watermark_columns, wm_prev)

    dedup_keys = plan.merge_keys or plan.hash_keys
    if plan.dedup_order_expr and dedup_keys:
        df = deduplicate_by_order(df, dedup_keys, plan.dedup_order_expr)

    df = fix_encoding(df, plan.fix_encoding, plan.encoding, plan.encoding_columns)
    df = (
        df.withColumn("ingestion_date", F.to_date(F.lit(run_date)))
        .withColumn("source_system", F.lit(plan.source_system))
        .withColumn("__run_id", F.lit(run_id))
    )
    return df


def _validate_plan(
    plan: IngestionPlan,
    df: DataFrame,
    target: str,
    apply_changes: bool = True,
) -> Dict[str, Any]:
    """Aplica regras de negócio do plan e valida schema policy.

    Regras de modo por layer:
        - bronze rejeita ``scd1_upsert``, ``scd2_historical``, ``snapshot_soft_delete``;
        - SCD1/SCD2/snapshot exigem ``merge_keys``;
        - hash_diff exige ``hash_keys``;
        - snapshot_soft_delete rejeita ``watermark_columns`` ou ``filter_expression``
          (snapshot precisa ser completo para marcar ausentes corretamente).

    Verifica também que todas as colunas referenciadas no plan existem no
    DataFrame, e aplica ``validate_schema_policy`` + ``sync_delta_schema``.

    Args:
        plan: Plano de ingestão.
        df: DataFrame já preparado.
        target: Nome qualificado do destino.
        apply_changes: Quando ``False``, executa apenas validações sem
            ``sync_delta_schema`` (usado em ``dry_run`` para evitar ALTER TABLE).

    Returns:
        Dict com ``status``, ``added_columns``, ``removed_columns``, ``type_changes``.

    Raises:
        ValueError: ao violar restrições de modo/layer ou schema policy.
    """
    if plan.layer == "bronze" and plan.mode in {
        "scd1_upsert",
        "scd2_historical",
        "snapshot_soft_delete",
    }:
        raise ValueError(
            "Bronze deve ser orientada a captura. Use scd0_append, scd0_overwrite "
            "ou scd1_hash_diff apenas quando houver contrato explícito."
        )
    if plan.mode in {"scd1_upsert", "snapshot_soft_delete", "scd2_historical"} and not plan.merge_keys:
        raise ValueError(f"mode={plan.mode} requer merge_keys")
    if plan.mode == "scd1_hash_diff" and not plan.hash_keys:
        raise ValueError("mode=scd1_hash_diff requer hash_keys")
    if plan.mode == "snapshot_soft_delete":
        if plan.watermark_columns:
            raise ValueError(
                "snapshot_soft_delete exige snapshot completo. Remova watermark_columns "
                "ou troque o mode (ex.: scd1_upsert)."
            )
        if plan.filter_expression:
            raise ValueError(
                "snapshot_soft_delete exige snapshot completo. Remova filter_expression "
                "ou troque o mode (ex.: scd1_upsert)."
            )

    cols_to_validate = []
    cols_to_validate += plan.watermark_columns
    cols_to_validate += plan.merge_keys
    cols_to_validate += plan.hash_keys
    cols_to_validate += plan.hash_exclude_columns
    cols_to_validate += plan.cluster_columns
    cols_to_validate += plan.zorder_columns
    if plan.partition_column:
        cols_to_validate.append(plan.partition_column)
    if plan.merge_partition_column:
        cols_to_validate.append(plan.merge_partition_column)
    cols_to_validate += plan.scd2_change_columns
    if plan.scd2_effective_from_column:
        cols_to_validate.append(plan.scd2_effective_from_column)
    validate_cols(df, sorted(set(cols_to_validate)), "plan columns")

    schema_changes = validate_schema_policy(df, target, plan.schema_policy)
    if apply_changes:
        sync_delta_schema(df, target, schema_changes, plan.schema_policy)
    return schema_changes


def _build_dry_run_result(
    plan: IngestionPlan,
    run_id: str,
    target: str,
    source_name: str,
    rows_read: int,
    rows_quarantined: int,
    wm_prev: Optional[str],
    wm_candidate: Optional[str],
    quality_status: str,
    schema_changes: Dict[str, Any],
    started_dt: datetime,
    df: DataFrame,
) -> Dict[str, Any]:
    """Monta o payload de retorno quando ``plan.dry_run=True``.

    Inclui resultado das validações sem efetuar escrita: ``status="DRY_RUN"``,
    contagens, watermarks, partições afetadas e mudanças de schema previstas.
    """
    finished_dt = utc_now_ts()
    return {
        "status": "DRY_RUN",
        "run_id": run_id,
        "target_table": target,
        "source_table": source_name,
        "mode": plan.mode,
        "write_strategy": write_strategy(plan.mode),
        "rows_read": rows_read,
        "rows_effective": rows_read - rows_quarantined,
        "rows_written": 0,
        "rows_quarantined": rows_quarantined,
        "affected_partitions": affected_partition_values(
            df, plan.partition_column or plan.merge_partition_column
        ),
        "watermark_previous": wm_prev,
        "watermark_candidate": wm_candidate,
        "quality_status": quality_status,
        "schema_changes": schema_changes,
        "duration_seconds": (finished_dt - started_dt).total_seconds(),
        "explain_captured": plan.explain_mode,
        "openlineage_enabled": plan.openlineage_enabled,
    }


def _finalize_execution(
    tables: Dict[str, str],
    plan: IngestionPlan,
    run_id: str,
    run_ts: str,
    run_date: str,
    source_name: str,
    target: str,
    status: str,
    started_dt: datetime,
    finished_dt: datetime,
    rows_read: int,
    rows_written: int,
    rows_quarantined: int,
    wm_prev: Optional[str],
    wm_current: Optional[str],
    quality_status: str,
    schema_changes: Dict[str, Any],
    operation_metrics: Dict[str, Any],
    write_started_at: Optional[str],
    write_finished_at: Optional[str],
    delta_version_before: Optional[int],
    delta_version_after: Optional[int],
    write_committed: bool,
    error: Optional[str],
    row_metrics: Dict[str, int],
) -> None:
    """Monta o payload completo e grava em ``ctrl_ingestion_runs`` via ``log_run``.

    Chamado no ``finally`` do orquestrador — sempre executa, mesmo em falha.
    """
    duration = (finished_dt - started_dt).total_seconds()
    log_run(
        tables,
        {
            "run_id": run_id,
            "run_ts_utc": run_ts,
            "run_date": run_date,
            "notebook_name": plan.notebook_name,
            "layer": plan.layer,
            "source_table": source_name,
            "target_table": target,
            "mode": plan.mode,
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_inserted": row_metrics.get("rows_inserted", 0),
            "rows_updated": row_metrics.get("rows_updated", 0),
            "rows_deleted": row_metrics.get("rows_deleted", 0),
            "rows_quarantined": rows_quarantined,
            "watermark_column": "|".join(plan.watermark_columns) if plan.watermark_columns else None,
            "watermark_previous": wm_prev,
            "watermark_current": wm_current,
            "started_at_utc": started_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at_utc": finished_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
            "quality_status": quality_status,
            "schema_policy": plan.schema_policy,
            "schema_changes_json": to_json(schema_changes),
            "operation_metrics_json": to_json(operation_metrics),
            "write_started_at_utc": write_started_at,
            "write_finished_at_utc": write_finished_at,
            "delta_version_before": delta_version_before,
            "delta_version_after": delta_version_after,
            "write_committed": write_committed,
            "error_message": error,
            "parent_run_id": plan.parent_run_id,
            "run_group_id": plan.run_group_id,
            "master_job_id": plan.master_job_id,
            "master_run_id": plan.master_run_id,
        },
    )


def ingest_plan(plan: IngestionPlan) -> Dict[str, Any]:
    """Orquestra a execução de um ``IngestionPlan`` completo.

    Fluxo (try/except/finally garante que ctrl tables sempre recebem o registro):

    1. Cria ctrl tables se necessário.
    2. Adquire lock se ``plan.lock_enabled``.
    3. Resolve a fonte e lê o watermark anterior.
    4. Prepara o DataFrame (select, filter, custom keys, watermark, dedup, encoding).
    5. Valida schema policy e regras do plan.
    6. Avalia quality gates.
    7. Em ``dry_run``, retorna sem escrever.
    8. Executa o motor de escrita correspondente, com retry para conflitos Delta.
    9. Atualiza ``ctrl_ingestion_state``.
    10. Em falha, registra ``status=FAILED`` mantendo watermark anterior.
    11. Sempre: persiste em ``ctrl_ingestion_runs`` e (se habilitado) emite OpenLineage.

    Returns:
        Dict com status, run_id, contagens, watermarks, mudanças de schema,
        métricas Delta, evento OpenLineage e mensagem de erro (se houver).
    """
    run_id = new_run_id()
    run_ts = utc_now_str()
    run_date = today_str()
    started_dt = utc_now_ts()
    target = full_table_name(plan.catalog, plan.layer, plan.target_table)
    if plan.dry_run:
        tables = ctrl_table_names(plan.catalog, plan.ctrl_schema)
    else:
        tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)

    source_name = "unknown"
    wm_prev: Optional[str] = None
    wm_current: Optional[str] = None
    rows_read = 0
    rows_written = 0
    rows_quarantined = 0
    status = "SUCCESS"
    quality_status = "NOT_CONFIGURED"
    schema_changes: Dict[str, Any] = {}
    operation_metrics: Dict[str, Any] = {}
    explain_text: Optional[str] = None
    openlineage_event: Optional[Dict[str, Any]] = None
    write_started_at: Optional[str] = None
    write_finished_at: Optional[str] = None
    write_committed = False
    delta_version_before: Optional[int] = None
    delta_version_after: Optional[int] = None
    wm_candidate: Optional[str] = None
    error: Optional[str] = None
    prepared_df: Optional[DataFrame] = None
    row_metrics: Dict[str, int] = {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

    try:
        if plan.lock_enabled and not plan.dry_run:
            acquire_lock(tables, target, run_id)

        raw_df, source_name = _resolve_source(plan)
        wm_prev = (
            get_watermark(tables["state"], target, plan.watermark_columns)
            if plan.watermark_columns
            else None
        )
        prepared_df = _prepare_dataframe(raw_df, plan, run_id, run_date, wm_prev)
        prepared_df = safe_cache(prepared_df, plan.use_cache)

        schema_changes = _validate_plan(plan, prepared_df, target, apply_changes=not plan.dry_run)
        rows_read = prepared_df.count()
        wm_candidate = (
            compute_watermark(prepared_df, plan.watermark_columns)
            if plan.watermark_columns and rows_read > 0
            else wm_prev
        )
        if plan.explain_mode:
            explain_text = capture_explain(prepared_df, plan.explain_format)
            if not plan.dry_run:
                write_explain_plan(
                    tables, run_id, target, source_name, plan.mode, plan.explain_format, explain_text
                )

        quality_status, quality_results, valid_df, quarantined_df, rows_quarantined = (
            evaluate_quality(prepared_df, plan.quality_rules, run_id, target)
        )
        if not plan.dry_run:
            write_quality_results(tables, run_id, target, quality_results)

        if quality_status == "FAILED":
            effective_action = plan.on_quality_fail
            abort_only_failed = [r for r in quality_results if is_abort_only_failure(r["rule_name"])]
            if effective_action == "quarantine" and abort_only_failed:
                names = sorted({r["rule_name"] for r in abort_only_failed})
                logger.warning(
                    f"Regras abortivas {names} não são quarentenáveis em nível de linha. "
                    "Escalando on_quality_fail de 'quarantine' para 'fail'."
                )
                effective_action = "fail"

            if effective_action == "fail":
                raise ValueError(f"Quality gates falharam: {to_json(quality_results)}")
            if effective_action == "quarantine":
                if not plan.dry_run:
                    write_quarantine(
                        tables, quarantined_df, run_id, target, "quality_gate", to_json(quality_results)
                    )
                prepared_df = valid_df
            elif effective_action == "warn":
                logger.warning(
                    f"Quality gates falharam, mas execução continuará: {to_json(quality_results)}"
                )

        if plan.dry_run:
            return _build_dry_run_result(
                plan, run_id, target, source_name, rows_read, rows_quarantined, wm_prev,
                wm_candidate, quality_status, schema_changes, started_dt, prepared_df,
            )

        effective_rows = (
            rows_read - rows_quarantined
            if quality_status == "FAILED" and plan.on_quality_fail == "quarantine"
            else rows_read
        )
        delta_version_before = delta_version(target) if table_exists(target) else None
        write_started_at = utc_now_str()
        rows_written = with_retry(
            lambda: execute_write_mode(plan, prepared_df, target, effective_rows)
        )
        write_finished_at = utc_now_str()
        delta_version_after = delta_version(target) if table_exists(target) else None
        write_committed = rows_written > 0 and delta_version_after != delta_version_before

        if rows_written > 0 and plan.optimize_after_write:
            run_optimize(target, plan.zorder_columns)

        wm_current = (
            compute_watermark(prepared_df, plan.watermark_columns)
            if plan.watermark_columns and rows_read > 0
            else wm_prev
        )
        operation_metrics = latest_operation_metrics(target) if table_exists(target) else {}
        delta_version_after = operation_metrics.get("version", delta_version_after)
        row_metrics = extract_row_metrics(operation_metrics)
        upsert_state(
            tables,
            target,
            "|".join(plan.watermark_columns) if plan.watermark_columns else None,
            wm_current,
            utc_now_str(),
            run_id,
            "SUCCESS",
            rows_written,
            None,
            plan,
            delta_version=delta_version_after,
            write_completed_at=write_finished_at,
            watermark_candidate=wm_candidate,
        )

    except Exception as exc:
        status = "FAILED"
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logger.error(f"Ingestão falhou: {exc}")
        if not plan.dry_run:
            try:
                upsert_state(
                    tables,
                    target,
                    "|".join(plan.watermark_columns) if plan.watermark_columns else None,
                    wm_prev,
                    None,
                    run_id,
                    "FAILED",
                    0,
                    error,
                    plan,
                    delta_version=delta_version_after,
                    write_completed_at=write_finished_at,
                    watermark_candidate=wm_candidate,
                )
            except Exception as state_exc:
                logger.error(f"Falha ao atualizar tabela de estado: {state_exc}")
        row_metrics = {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}
    finally:
        if prepared_df is not None:
            safe_unpersist(prepared_df, plan.use_cache)
        if plan.lock_enabled and not plan.dry_run:
            release_lock(tables, target, run_id)
        if not plan.dry_run:
            finished_dt = utc_now_ts()
            try:
                _finalize_execution(
                    tables, plan, run_id, run_ts, run_date, source_name, target, status, started_dt,
                    finished_dt, rows_read, rows_written, rows_quarantined, wm_prev, wm_current,
                    quality_status, schema_changes, operation_metrics, write_started_at,
                    write_finished_at, delta_version_before, delta_version_after, write_committed,
                    error, row_metrics,
                )
            except Exception as log_exc:
                logger.error(f"Falha ao registrar execução: {log_exc}")
            try:
                output_df = spark.read.table(target) if table_exists(target) else None
                openlineage_event = write_openlineage_event(
                    tables, plan, run_id, target, source_name, status, started_dt, finished_dt,
                    prepared_df, output_df, rows_read, rows_written, delta_version_before,
                    delta_version_after, operation_metrics,
                )
            except Exception as lineage_exc:
                logger.error(f"Falha ao registrar evento OpenLineage: {lineage_exc}")

    return {
        "status": status,
        "run_id": run_id,
        "target_table": target,
        "source_table": source_name,
        "mode": plan.mode,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "watermark_previous": wm_prev,
        "watermark_current": wm_current,
        "quality_status": quality_status,
        "schema_changes": schema_changes,
        "operation_metrics": operation_metrics,
        "write_committed": write_committed,
        "delta_version_before": delta_version_before,
        "delta_version_after": delta_version_after,
        "write_delta_version": delta_version_after if write_committed else None,
        "explain_captured": bool(explain_text),
        "openlineage_event_emitted": bool(openlineage_event),
        "openlineage_event": openlineage_event,
        "error_message": safe_truncate(error, 2000),
    }


def ingest(**kwargs: Any) -> Dict[str, Any]:
    """Executa ingestão padronizada usando parâmetros compatíveis com notebooks.

    Exemplo:
        ingest(
            source=df,
            target_table="c_cliente",
            catalog="sandbox_catalog1",
            layer="silver",
            mode="scd1_upsert",
            merge_keys="id_cliente",
            watermark_columns="updated_at",
            dedup_order_expr="updated_at DESC NULLS LAST",
            schema_policy="additive_only",
            quality_rules={"not_null": ["id_cliente"], "unique_key": ["id_cliente"]},
            explain_mode=True,
            openlineage_enabled=True,
        )
    """
    plan = build_plan_from_kwargs(**kwargs)
    return ingest_plan(plan)


EXAMPLE_BRONZE_PLAN = {
    "source": "raw_orders",
    "target_table": "b_orders",
    "catalog": "main",
    "layer": "bronze",
    "mode": "scd0_append",
    "watermark_columns": "updated_at",
    "schema_policy": "permissive",
}

EXAMPLE_SILVER_PLAN = {
    "source": "b_orders",
    "target_table": "c_orders",
    "catalog": "main",
    "layer": "silver",
    "mode": "scd1_upsert",
    "merge_keys": "order_id",
    "watermark_columns": "updated_at",
    "dedup_order_expr": "updated_at DESC NULLS LAST",
    "schema_policy": "additive_only",
    "quality_rules": {
        "required_columns": ["order_id", "updated_at"],
        "not_null": ["order_id"],
        "unique_key": ["order_id"],
        "min_rows": 1,
    },
}
