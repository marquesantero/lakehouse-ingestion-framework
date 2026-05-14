"""Tabelas de controle, log de runs, upsert de estado, locks e retry."""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .config import CONFIG, CTRL_SCHEMA_VERSION, FRAMEWORK_VERSION
from .plan import IngestionPlan
from ._spark import spark
from ._sql import full_table_name, q, qt, safe_truncate, sql_int, sql_lit, to_json, utc_now_str

logger = logging.getLogger("lakehouse_ingestion")


def ctrl_table_names(catalog: str, schema: str) -> Dict[str, str]:
    """Calcula apenas os nomes qualificados das ctrl tables, sem criar nada.

    Útil em ``dry_run`` para obter o dict de referência sem efeito colateral
    (sem ``CREATE SCHEMA``/``CREATE TABLE``).
    """
    return {
        "runs": full_table_name(catalog, schema, CONFIG.ctrl_table_runs),
        "state": full_table_name(catalog, schema, CONFIG.ctrl_table_state),
        "quality": full_table_name(catalog, schema, CONFIG.ctrl_table_quality),
        "quarantine": full_table_name(catalog, schema, CONFIG.ctrl_table_quarantine),
        "locks": full_table_name(catalog, schema, CONFIG.ctrl_table_locks),
        "explain": full_table_name(catalog, schema, CONFIG.ctrl_table_explain),
        "lineage": full_table_name(catalog, schema, CONFIG.ctrl_table_lineage),
        "metadata": full_table_name(catalog, schema, CONFIG.ctrl_table_metadata),
        "errors": full_table_name(catalog, schema, CONFIG.ctrl_table_errors),
        "schema_changes": full_table_name(catalog, schema, CONFIG.ctrl_table_schema_changes),
        "streams": full_table_name(catalog, schema, CONFIG.ctrl_table_streams),
        "annotations": full_table_name(catalog, schema, CONFIG.ctrl_table_annotations),
        "operations": full_table_name(catalog, schema, CONFIG.ctrl_table_operations),
        "access": full_table_name(catalog, schema, CONFIG.ctrl_table_access),
    }


def _table_columns(table: str) -> set[str]:
    try:
        return {field.name for field in spark.read.table(table).schema.fields}
    except Exception:
        return set()


def _add_columns_if_missing(table: str, columns: Dict[str, str]) -> None:
    existing = _table_columns(table)
    missing = {name: dtype for name, dtype in columns.items() if name not in existing}
    if not missing:
        return
    cols_sql = ", ".join(f"{q(name)} {dtype}" for name, dtype in missing.items())
    spark.sql(f"ALTER TABLE {qt(table)} ADD COLUMNS ({cols_sql})")


def _record_ctrl_metadata(tables: Dict[str, str]) -> None:
    spark.sql(f"""
        MERGE INTO {qt(tables['metadata'])} t
        USING (
            SELECT
                'lakehouse_ingestion' AS component,
                {sql_lit(FRAMEWORK_VERSION)} AS framework_version,
                {sql_int(CTRL_SCHEMA_VERSION)} AS ctrl_schema_version,
                current_timestamp() AS updated_at_utc
        ) s
        ON t.component = s.component
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def ensure_ctrl_tables(catalog: str, schema: str) -> Dict[str, str]:
    """Cria (idempotente) o schema e as tabelas de controle.

    Retorna um dict com nomes lógicos -> nomes qualificados:

    - ``runs`` (particionada por ``run_date``)
    - ``state`` (uma linha por ``target_table``, PK)
    - ``quality`` (uma linha por regra falhada por execução)
    - ``quarantine`` (uma linha por linha quarentenada)
    - ``locks`` (uma linha por ``target_table``, PK)
    - ``explain`` (planos Spark capturados)
    - ``lineage`` (eventos OpenLineage como JSON)
    - ``metadata`` (versão do framework e do schema de controle)
    - ``errors`` (stack traces completos para diagnóstico)

    Migra apenas colunas aditivas conhecidas. Nunca remove colunas automaticamente.
    """
    tables = ctrl_table_names(catalog, schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q(catalog)}.{q(schema)}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['runs'])} (
            run_id STRING,
            run_ts_utc TIMESTAMP,
            run_date DATE,
            notebook_name STRING,
            layer STRING,
            source_table STRING,
            source_type STRING,
            source_connector STRING,
            source_name STRING,
            source_provider STRING,
            source_format STRING,
            source_path STRING,
            source_options_json STRING,
            source_read_json STRING,
            source_request_json STRING,
            source_auth_json STRING,
            source_pagination_json STRING,
            source_response_json STRING,
            source_incremental_json STRING,
            source_limits_json STRING,
            source_capabilities_json STRING,
            source_metrics_json STRING,
            target_table STRING,
            mode STRING,
            status STRING,
            rows_read BIGINT,
            rows_written BIGINT,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            rows_deleted BIGINT,
            rows_quarantined BIGINT,
            watermark_column STRING,
            watermark_previous STRING,
            watermark_current STRING,
            started_at_utc TIMESTAMP,
            finished_at_utc TIMESTAMP,
            duration_seconds DOUBLE,
            quality_status STRING,
            schema_policy STRING,
            schema_changes_json STRING,
            stage_durations_json STRING,
            contract_description STRING,
            contract_owner STRING,
            contract_domain STRING,
            contract_tags_json STRING,
            contract_sla STRING,
            runtime_parameters_json STRING,
            operation_metrics_json STRING,
            write_started_at_utc TIMESTAMP,
            write_finished_at_utc TIMESTAMP,
            delta_version_before BIGINT,
            delta_version_after BIGINT,
            write_committed BOOLEAN,
            error_message STRING,
            parent_run_id STRING,
            run_group_id STRING,
            master_job_id STRING,
            master_run_id STRING,
            idempotency_key STRING,
            idempotency_policy STRING,
            skip_reason STRING,
            skipped_by_run_id STRING,
            metrics_source STRING,
            framework_version STRING,
            ctrl_schema_version BIGINT,
            runtime_type STRING,
            spark_version STRING,
            python_version STRING,
            annotations_status STRING,
            annotations_result_json STRING,
            ownership_json STRING,
            operations_json STRING
        ) USING DELTA PARTITIONED BY (run_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['state'])} (
            target_table STRING NOT NULL,
            watermark_column STRING,
            watermark_value STRING,
            last_success_at_utc TIMESTAMP,
            last_run_id STRING,
            last_status STRING,
            last_rows_written BIGINT,
            last_error_message STRING,
            parent_run_id STRING,
            run_group_id STRING,
            master_job_id STRING,
            master_run_id STRING,
            last_delta_version BIGINT,
            last_write_completed_at_utc TIMESTAMP,
            last_watermark_candidate STRING,
            last_updated_at_utc TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['quality'])} (
            run_id STRING,
            target_table STRING,
            rule_name STRING,
            status STRING,
            severity STRING,
            failed_count BIGINT,
            checked_at_utc TIMESTAMP,
            message STRING,
            details_json STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['quarantine'])} (
            run_id STRING,
            target_table STRING,
            rule_name STRING,
            error_reason STRING,
            record_payload STRING,
            quarantined_at_utc TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['locks'])} (
            target_table STRING NOT NULL,
            run_id STRING,
            owner STRING,
            acquired_at_utc TIMESTAMP,
            expires_at_utc TIMESTAMP,
            ttl_minutes BIGINT,
            released_at_utc TIMESTAMP,
            status STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['explain'])} (
            run_id STRING,
            target_table STRING,
            source_table STRING,
            mode STRING,
            explain_format STRING,
            plan_text STRING,
            captured_at_utc TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['lineage'])} (
            run_id STRING,
            event_time_utc TIMESTAMP,
            event_type STRING,
            target_table STRING,
            source_table STRING,
            namespace STRING,
            producer STRING,
            event_json STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['metadata'])} (
            component STRING NOT NULL,
            framework_version STRING,
            ctrl_schema_version BIGINT,
            updated_at_utc TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['errors'])} (
            run_id STRING,
            error_ts_utc TIMESTAMP,
            error_date DATE,
            target_table STRING,
            source_table STRING,
            mode STRING,
            status STRING,
            error_type STRING,
            error_message STRING,
            stack_trace STRING,
            framework_version STRING,
            ctrl_schema_version BIGINT,
            runtime_type STRING,
            spark_version STRING,
            python_version STRING
        ) USING DELTA PARTITIONED BY (error_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['schema_changes'])} (
            run_id STRING,
            change_ts_utc TIMESTAMP,
            target_table STRING,
            change_type STRING,
            column_name STRING,
            source_type STRING,
            target_type STRING,
            applied BOOLEAN,
            details_json STRING,
            framework_version STRING,
            ctrl_schema_version BIGINT
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['streams'])} (
            stream_run_id STRING,
            idempotency_key STRING,
            idempotency_policy STRING,
            skip_reason STRING,
            skipped_by_stream_run_id STRING,
            target_table STRING,
            target_catalog STRING,
            target_layer STRING,
            notebook_name STRING,
            source_type STRING,
            source_path STRING,
            trigger STRING,
            checkpoint_location STRING,
            status STRING,
            started_at_utc TIMESTAMP,
            ended_at_utc TIMESTAMP,
            duration_seconds DOUBLE,
            batches_processed BIGINT,
            total_rows_read BIGINT,
            total_rows_written BIGINT,
            total_rows_quarantined BIGINT,
            framework_version STRING,
            ctrl_schema_version BIGINT,
            runtime_type STRING,
            spark_version STRING,
            python_version STRING,
            error_message STRING,
            master_job_id STRING,
            master_run_id STRING,
            parent_run_id STRING,
            run_group_id STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['annotations'])} (
            run_id STRING,
            target_table STRING,
            annotation_scope STRING,
            annotation_type STRING,
            column_name STRING,
            key STRING,
            previous_value STRING,
            value STRING,
            status STRING,
            error_message STRING,
            applied_sql STRING,
            annotation_ts_utc TIMESTAMP,
            annotation_date DATE,
            framework_version STRING,
            ctrl_schema_version BIGINT
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['operations'])} (
            run_id STRING,
            target_table STRING,
            criticality STRING,
            expected_frequency STRING,
            freshness_sla_minutes BIGINT,
            alert_on_failure BOOLEAN,
            alert_on_quality_fail BOOLEAN,
            runbook_url STRING,
            ownership_json STRING,
            owners_json STRING,
            groups_json STRING,
            tags_json STRING,
            status STRING,
            recorded_at_utc TIMESTAMP,
            framework_version STRING,
            ctrl_schema_version BIGINT
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['access'])} (
            access_run_id STRING,
            run_id STRING,
            target_table STRING,
            access_type STRING,
            principal STRING,
            privilege STRING,
            column_name STRING,
            function_name STRING,
            object_name STRING,
            status STRING,
            error_message STRING,
            applied_sql STRING,
            previous_value STRING,
            new_value STRING,
            mode STRING,
            drift_policy STRING,
            revoke_unmanaged BOOLEAN,
            access_ts_utc TIMESTAMP,
            access_date DATE,
            framework_version STRING,
            ctrl_schema_version BIGINT
        ) USING DELTA
    """)
    _add_columns_if_missing(
        tables["runs"],
        {
            "idempotency_key": "STRING",
            "idempotency_policy": "STRING",
            "skip_reason": "STRING",
            "skipped_by_run_id": "STRING",
            "metrics_source": "STRING",
            "framework_version": "STRING",
            "ctrl_schema_version": "BIGINT",
            "runtime_type": "STRING",
            "spark_version": "STRING",
            "python_version": "STRING",
            "stage_durations_json": "STRING",
            "contract_description": "STRING",
            "contract_owner": "STRING",
            "contract_domain": "STRING",
            "contract_tags_json": "STRING",
            "contract_sla": "STRING",
            "runtime_parameters_json": "STRING",
            "annotations_status": "STRING",
            "annotations_result_json": "STRING",
            "ownership_json": "STRING",
            "operations_json": "STRING",
            "source_type": "STRING",
            "source_connector": "STRING",
            "source_name": "STRING",
            "source_provider": "STRING",
            "source_format": "STRING",
            "source_path": "STRING",
            "source_options_json": "STRING",
            "source_read_json": "STRING",
            "source_request_json": "STRING",
            "source_auth_json": "STRING",
            "source_pagination_json": "STRING",
            "source_response_json": "STRING",
            "source_incremental_json": "STRING",
            "source_limits_json": "STRING",
            "source_capabilities_json": "STRING",
            "source_metrics_json": "STRING",
        },
    )
    _add_columns_if_missing(
        tables["locks"],
        {
            "owner": "STRING",
            "ttl_minutes": "BIGINT",
            "released_at_utc": "TIMESTAMP",
        },
    )
    _add_columns_if_missing(
        tables["quality"],
        {
            "severity": "STRING",
            "message": "STRING",
        },
    )
    _add_columns_if_missing(
        tables["annotations"],
        {
            "previous_value": "STRING",
            "annotation_date": "DATE",
        },
    )
    _add_columns_if_missing(
        tables["operations"],
        {
            "ownership_json": "STRING",
        },
    )
    _add_columns_if_missing(
        tables["access"],
        {
            "access_run_id": "STRING",
            "column_name": "STRING",
            "function_name": "STRING",
            "new_value": "STRING",
            "mode": "STRING",
            "drift_policy": "STRING",
            "revoke_unmanaged": "BOOLEAN",
            "access_date": "DATE",
        },
    )
    _record_ctrl_metadata(tables)
    return tables


_ANNOTATION_COLUMNS = [
    "run_id", "target_table", "annotation_scope", "annotation_type", "column_name",
    "key", "previous_value", "value", "status", "error_message", "applied_sql",
    "annotation_ts_utc", "annotation_date", "framework_version", "ctrl_schema_version",
]


def log_annotation_entries(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    entries: list[Dict[str, Any]],
) -> None:
    """Audita aplicacao de comments/tags em ``ctrl_ingestion_annotations``."""
    for entry in entries:
        payload = {
            **entry,
            "run_id": run_id,
            "target_table": target_table,
            "applied_sql": entry.get("sql"),
            "annotation_ts_utc": utc_now_str(),
            "annotation_date": utc_now_str()[:10],
            "framework_version": FRAMEWORK_VERSION,
            "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        }
        values = []
        for column in _ANNOTATION_COLUMNS:
            value = payload.get(column)
            if column == "ctrl_schema_version":
                values.append(sql_int(value))
            elif column.endswith("_utc"):
                values.append(f"CAST({sql_lit(value)} AS TIMESTAMP)")
            elif column.endswith("_date"):
                values.append(f"CAST({sql_lit(value)} AS DATE)")
            elif column == "error_message":
                values.append(sql_lit(safe_truncate(value, 2000)))
            else:
                values.append(sql_lit(value))
        spark.sql(
            f"INSERT INTO {qt(tables['annotations'])} ({', '.join(_ANNOTATION_COLUMNS)}) "
            f"VALUES ({', '.join(values)})"
        )


_OPERATION_COLUMNS = [
    "run_id", "target_table", "criticality", "expected_frequency", "freshness_sla_minutes",
    "alert_on_failure", "alert_on_quality_fail", "runbook_url", "ownership_json", "owners_json",
    "groups_json", "tags_json", "status", "recorded_at_utc", "framework_version", "ctrl_schema_version",
]


def log_operations_contract(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    payload: Dict[str, Any],
) -> None:
    """Audita contrato operacional em ``ctrl_ingestion_operations``."""
    enriched = {
        **payload,
        "run_id": run_id,
        "target_table": target_table,
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
    }
    values = []
    for column in _OPERATION_COLUMNS:
        value = enriched.get(column)
        if column in {"freshness_sla_minutes", "ctrl_schema_version"}:
            values.append(sql_int(value))
        elif column in {"alert_on_failure", "alert_on_quality_fail"}:
            values.append("NULL" if value is None else str(bool(value)).lower())
        elif column.endswith("_utc"):
            values.append(f"CAST({sql_lit(value)} AS TIMESTAMP)")
        else:
            values.append(sql_lit(value))
    spark.sql(
        f"INSERT INTO {qt(tables['operations'])} ({', '.join(_OPERATION_COLUMNS)}) "
        f"VALUES ({', '.join(values)})"
    )


_ACCESS_COLUMNS = [
    "access_run_id", "run_id", "target_table", "access_type", "principal", "privilege",
    "column_name", "function_name", "object_name", "status", "error_message", "applied_sql",
    "previous_value", "new_value", "mode", "drift_policy", "revoke_unmanaged", "access_ts_utc",
    "access_date", "framework_version", "ctrl_schema_version",
]


def log_access_entries(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    entries: list[Dict[str, Any]],
) -> None:
    """Audita grants, row filters e masks em ``ctrl_ingestion_access``."""
    for entry in entries:
        payload = {
            **entry,
            "access_run_id": run_id,
            "run_id": run_id,
            "target_table": target_table,
            "applied_sql": entry.get("sql"),
            "access_ts_utc": utc_now_str(),
            "access_date": utc_now_str()[:10],
            "framework_version": FRAMEWORK_VERSION,
            "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        }
        values = []
        for column in _ACCESS_COLUMNS:
            value = payload.get(column)
            if column == "ctrl_schema_version":
                values.append(sql_int(value))
            elif column == "revoke_unmanaged":
                values.append("NULL" if value is None else str(bool(value)).lower())
            elif column.endswith("_utc"):
                values.append(f"CAST({sql_lit(value)} AS TIMESTAMP)")
            elif column.endswith("_date"):
                values.append(f"CAST({sql_lit(value)} AS DATE)")
            elif column == "error_message":
                values.append(sql_lit(safe_truncate(value, 2000)))
            else:
                values.append(sql_lit(value))
        spark.sql(
            f"INSERT INTO {qt(tables['access'])} ({', '.join(_ACCESS_COLUMNS)}) "
            f"VALUES ({', '.join(values)})"
        )


_STREAM_COLUMNS = [
    "stream_run_id", "idempotency_key", "idempotency_policy", "skip_reason",
    "skipped_by_stream_run_id", "target_table", "target_catalog", "target_layer",
    "notebook_name", "source_type", "source_path", "trigger", "checkpoint_location",
    "status", "started_at_utc", "ended_at_utc", "duration_seconds", "batches_processed",
    "total_rows_read", "total_rows_written", "total_rows_quarantined", "framework_version",
    "ctrl_schema_version", "runtime_type", "spark_version", "python_version", "error_message",
    "master_job_id", "master_run_id", "parent_run_id", "run_group_id",
]
_STREAM_INT_COLUMNS = {
    "batches_processed", "total_rows_read", "total_rows_written", "total_rows_quarantined",
    "ctrl_schema_version",
}


def log_stream_start(tables: Dict[str, str], payload: Dict[str, Any]) -> None:
    """Insere início de execução em ``ctrl_ingestion_streams``."""
    values = []
    for c in _STREAM_COLUMNS:
        v = payload.get(c)
        if c in _STREAM_INT_COLUMNS:
            values.append(sql_int(v))
        elif c == "duration_seconds":
            values.append("NULL" if v is None else str(float(v)))
        elif c.endswith("_utc"):
            values.append(f"CAST({sql_lit(v)} AS TIMESTAMP)")
        elif c == "error_message":
            values.append(sql_lit(safe_truncate(v, 2000)))
        else:
            values.append(sql_lit(v))
    spark.sql(
        f"INSERT INTO {qt(tables['streams'])} ({', '.join(_STREAM_COLUMNS)}) "
        f"VALUES ({', '.join(values)})"
    )


def log_stream_finish(tables: Dict[str, str], stream_run_id: str, payload: Dict[str, Any]) -> None:
    """Atualiza fim de execução em ``ctrl_ingestion_streams``."""
    assignments = []
    for c in _STREAM_COLUMNS:
        if c == "stream_run_id" or c not in payload:
            continue
        v = payload.get(c)
        if c in _STREAM_INT_COLUMNS:
            rendered = sql_int(v)
        elif c == "duration_seconds":
            rendered = "NULL" if v is None else str(float(v))
        elif c.endswith("_utc"):
            rendered = f"CAST({sql_lit(v)} AS TIMESTAMP)"
        elif c == "error_message":
            rendered = sql_lit(safe_truncate(v, 2000))
        else:
            rendered = sql_lit(v)
        assignments.append(f"{q(c)} = {rendered}")
    if not assignments:
        return
    spark.sql(
        f"UPDATE {qt(tables['streams'])} SET {', '.join(assignments)} "
        f"WHERE stream_run_id = {sql_lit(stream_run_id)}"
    )


def stream_child_run_metrics(tables: Dict[str, str], stream_run_id: str) -> Dict[str, int]:
    """Agrega métricas dos runs filhos de um ``ingest_stream_plan``.

    Em Spark Connect/serverless, o callback de ``foreachBatch`` pode executar
    em outro contexto Python. Nesses casos, mutações em listas locais do driver
    nem sempre ficam visíveis após ``awaitTermination``. A fonte confiável passa
    a ser ``ctrl_ingestion_runs.parent_run_id``.
    """
    try:
        row = (
            spark.read.table(tables["runs"])
            .where(F.col("parent_run_id") == stream_run_id)
            .agg(
                F.count(F.lit(1)).alias("batches_processed"),
                F.sum(F.coalesce(F.col("rows_read"), F.lit(0))).alias("total_rows_read"),
                F.sum(F.coalesce(F.col("rows_written"), F.lit(0))).alias("total_rows_written"),
                F.sum(F.coalesce(F.col("rows_quarantined"), F.lit(0))).alias(
                    "total_rows_quarantined"
                ),
            )
            .first()
        )
        if row is None:
            return {
                "batches_processed": 0,
                "total_rows_read": 0,
                "total_rows_written": 0,
                "total_rows_quarantined": 0,
            }
        return {
            "batches_processed": int(row["batches_processed"] or 0),
            "total_rows_read": int(row["total_rows_read"] or 0),
            "total_rows_written": int(row["total_rows_written"] or 0),
            "total_rows_quarantined": int(row["total_rows_quarantined"] or 0),
        }
    except Exception:
        return {
            "batches_processed": 0,
            "total_rows_read": 0,
            "total_rows_written": 0,
            "total_rows_quarantined": 0,
        }


_RUN_COLUMNS = [
    "run_id", "run_ts_utc", "run_date", "notebook_name", "layer", "source_table",
    "source_type", "source_connector", "source_name", "source_provider", "source_format",
    "source_path", "source_options_json", "source_read_json", "source_request_json",
    "source_auth_json", "source_pagination_json", "source_response_json",
    "source_incremental_json", "source_limits_json", "source_capabilities_json",
    "source_metrics_json",
    "target_table", "mode", "status", "rows_read", "rows_written", "rows_inserted",
    "rows_updated", "rows_deleted", "rows_quarantined", "watermark_column",
    "watermark_previous", "watermark_current", "started_at_utc", "finished_at_utc",
    "duration_seconds", "quality_status", "schema_policy", "schema_changes_json",
    "stage_durations_json", "contract_description", "contract_owner", "contract_domain",
    "contract_tags_json", "contract_sla", "runtime_parameters_json", "operation_metrics_json",
    "write_started_at_utc", "write_finished_at_utc", "delta_version_before",
    "delta_version_after", "write_committed", "error_message", "parent_run_id",
    "run_group_id", "master_job_id", "master_run_id", "idempotency_key",
    "idempotency_policy", "skip_reason", "skipped_by_run_id", "metrics_source",
    "framework_version", "ctrl_schema_version", "runtime_type", "spark_version",
    "python_version", "annotations_status", "annotations_result_json", "ownership_json",
    "operations_json",
]
_RUN_INT_COLUMNS = {
    "rows_read", "rows_written", "rows_inserted", "rows_updated", "rows_deleted",
    "rows_quarantined", "delta_version_before", "delta_version_after", "ctrl_schema_version",
}


def log_run(tables: Dict[str, str], payload: Dict[str, Any]) -> None:
    """Insere uma linha em ``ctrl_ingestion_runs`` com tipos coercidos.

    Aceita um dict com chaves correspondentes a ``_RUN_COLUMNS``. Valores
    ``None`` viram ``NULL``. Strings de erro são truncadas em
    ``CONFIG.max_error_len``.
    """
    values = []
    for c in _RUN_COLUMNS:
        v = payload.get(c)
        if c in _RUN_INT_COLUMNS:
            values.append(sql_int(v))
        elif c == "write_committed":
            values.append("NULL" if v is None else str(bool(v)).lower())
        elif c == "duration_seconds":
            values.append("NULL" if v is None else str(float(v)))
        elif c == "run_date":
            values.append(f"CAST({sql_lit(v)} AS DATE)")
        elif c.endswith("_utc") or c in {"started_at_utc", "finished_at_utc"}:
            values.append(f"CAST({sql_lit(v)} AS TIMESTAMP)")
        else:
            values.append(sql_lit(safe_truncate(v) if c == "error_message" else v))
    spark.sql(
        f"INSERT INTO {qt(tables['runs'])} ({', '.join(_RUN_COLUMNS)}) "
        f"VALUES ({', '.join(values)})"
    )


_ERROR_COLUMNS = [
    "run_id", "error_ts_utc", "error_date", "target_table", "source_table", "mode",
    "status", "error_type", "error_message", "stack_trace", "framework_version",
    "ctrl_schema_version", "runtime_type", "spark_version", "python_version",
]


_SCHEMA_CHANGE_COLUMNS = [
    "run_id", "change_ts_utc", "target_table", "change_type", "column_name",
    "source_type", "target_type", "applied", "details_json", "framework_version",
    "ctrl_schema_version",
]


def log_schema_changes(
    tables: Dict[str, str],
    run_id: str,
    target: str,
    schema_changes: Dict[str, Any],
) -> None:
    """Persiste mudanças de schema detectadas/aplicadas na ctrl table dedicada."""
    rows = []
    for column in schema_changes.get("added_columns") or []:
        rows.append(
            {
                "change_type": "add_column",
                "column_name": column,
                "source_type": None,
                "target_type": None,
                "applied": True,
                "details": {},
            }
        )
    for change in schema_changes.get("type_changes") or []:
        rows.append(
            {
                "change_type": change.get("change", "type_change"),
                "column_name": change.get("column"),
                "source_type": change.get("source"),
                "target_type": change.get("target"),
                "applied": bool(change.get("applied")),
                "details": change,
            }
        )
    if not rows:
        return

    values_sql = []
    for row in rows:
        values_sql.append(
            "("
            + ", ".join(
                [
                    sql_lit(run_id),
                    "current_timestamp()",
                    sql_lit(target),
                    sql_lit(row["change_type"]),
                    sql_lit(row["column_name"]),
                    sql_lit(row["source_type"]),
                    sql_lit(row["target_type"]),
                    str(bool(row["applied"])).lower(),
                    sql_lit(to_json(row["details"])),
                    sql_lit(FRAMEWORK_VERSION),
                    sql_int(CTRL_SCHEMA_VERSION),
                ]
            )
            + ")"
        )
    spark.sql(
        f"INSERT INTO {qt(tables['schema_changes'])} ({', '.join(_SCHEMA_CHANGE_COLUMNS)}) "
        f"VALUES {', '.join(values_sql)}"
    )


def log_error(tables: Dict[str, str], payload: Dict[str, Any]) -> None:
    """Insere o erro completo em ``ctrl_ingestion_errors``.

    ``ctrl_ingestion_runs.error_message`` permanece curto para consulta rápida;
    esta tabela guarda o stack trace integral para suporte e auditoria.
    """
    values = []
    for c in _ERROR_COLUMNS:
        v = payload.get(c)
        if c == "ctrl_schema_version":
            values.append(sql_int(v))
        elif c == "error_date":
            values.append(f"CAST({sql_lit(v)} AS DATE)")
        elif c == "error_ts_utc":
            values.append(f"CAST({sql_lit(v)} AS TIMESTAMP)")
        elif c == "error_message":
            values.append(sql_lit(safe_truncate(v, 2000)))
        else:
            values.append(sql_lit(v))
    spark.sql(
        f"INSERT INTO {qt(tables['errors'])} ({', '.join(_ERROR_COLUMNS)}) "
        f"VALUES ({', '.join(values)})"
    )


def find_idempotent_run(
    tables: Dict[str, str],
    target: str,
    idempotency_key: Optional[str],
    status: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Retorna a execução mais recente para ``target`` + ``idempotency_key``."""
    if not idempotency_key:
        return None
    try:
        query = (
            spark.read.table(tables["runs"])
            .where(
                (F.col("target_table") == target)
                & (F.col("idempotency_key") == idempotency_key)
            )
        )
        if status:
            query = query.where(F.col("status") == status)
        row = (
            query.orderBy(F.col("run_ts_utc").desc_nulls_last())
            .select("run_id", "status")
            .limit(1)
            .first()
        )
        return None if row is None else row.asDict(recursive=True)
    except Exception:
        return None


def find_idempotent_stream(
    tables: Dict[str, str],
    target: str,
    idempotency_key: Optional[str],
    status: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Retorna stream mais recente para ``target`` + ``idempotency_key``."""
    if not idempotency_key:
        return None
    try:
        query = (
            spark.read.table(tables["streams"])
            .where(
                (F.col("target_table") == target)
                & (F.col("idempotency_key") == idempotency_key)
            )
        )
        if status:
            query = query.where(F.col("status") == status)
        row = (
            query.orderBy(F.col("started_at_utc").desc_nulls_last())
            .select("stream_run_id", "status")
            .limit(1)
            .first()
        )
        return None if row is None else row.asDict(recursive=True)
    except Exception:
        return None


def has_successful_run(tables: Dict[str, str], target: str, idempotency_key: Optional[str]) -> bool:
    """Indica se uma execução anterior com a mesma chave já terminou com sucesso."""
    previous = find_idempotent_run(tables, target, idempotency_key, status="SUCCESS")
    return bool(previous and previous.get("status") == "SUCCESS")


def upsert_state(
    tables: Dict[str, str],
    target: str,
    watermark_column: Optional[str],
    watermark_value: Optional[str],
    success_at: Optional[str],
    run_id: str,
    status: str,
    rows_written: int,
    error: Optional[str],
    plan: IngestionPlan,
    delta_version: Optional[int] = None,
    write_completed_at: Optional[str] = None,
    watermark_candidate: Optional[str] = None,
) -> None:
    """Atualiza a única linha de ``target_table`` em ``ctrl_ingestion_state``.

    Faz ``MERGE`` com ``WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT``. Em
    falha, é chamado com ``status="FAILED"`` e ``watermark_value=wm_prev``
    (não avança).
    """
    spark.sql(f"""
        MERGE INTO {qt(tables['state'])} t
        USING (
            SELECT
                {sql_lit(target)} AS target_table,
                {sql_lit(watermark_column)} AS watermark_column,
                {sql_lit(watermark_value)} AS watermark_value,
                CAST({sql_lit(success_at)} AS TIMESTAMP) AS last_success_at_utc,
                {sql_lit(run_id)} AS last_run_id,
                {sql_lit(status)} AS last_status,
                {sql_int(rows_written)} AS last_rows_written,
                {sql_lit(safe_truncate(error, 4000))} AS last_error_message,
                {sql_lit(plan.parent_run_id)} AS parent_run_id,
                {sql_lit(plan.run_group_id)} AS run_group_id,
                {sql_lit(plan.master_job_id)} AS master_job_id,
                {sql_lit(plan.master_run_id)} AS master_run_id,
                {sql_int(delta_version)} AS last_delta_version,
                CAST({sql_lit(write_completed_at)} AS TIMESTAMP) AS last_write_completed_at_utc,
                {sql_lit(watermark_candidate)} AS last_watermark_candidate,
                current_timestamp() AS last_updated_at_utc
        ) s
        ON t.target_table = s.target_table
        WHEN MATCHED THEN UPDATE SET
            t.watermark_column = s.watermark_column,
            t.watermark_value = s.watermark_value,
            t.last_success_at_utc = s.last_success_at_utc,
            t.last_run_id = s.last_run_id,
            t.last_status = s.last_status,
            t.last_rows_written = s.last_rows_written,
            t.last_error_message = s.last_error_message,
            t.parent_run_id = s.parent_run_id,
            t.run_group_id = s.run_group_id,
            t.master_job_id = s.master_job_id,
            t.master_run_id = s.master_run_id,
            t.last_delta_version = s.last_delta_version,
            t.last_write_completed_at_utc = s.last_write_completed_at_utc,
            t.last_watermark_candidate = s.last_watermark_candidate,
            t.last_updated_at_utc = s.last_updated_at_utc
        WHEN NOT MATCHED THEN INSERT *
    """)


def acquire_lock(
    tables: Dict[str, str],
    target: str,
    run_id: str,
    owner: Optional[str] = None,
    ttl_minutes: int = CONFIG.default_lock_ttl_minutes,
) -> None:
    """Tenta adquirir lock best-effort em ``target_table``.

    Faz MERGE em ``ctrl_ingestion_locks`` e em seguida lê para confirmar que
    este ``run_id`` ficou como ``ACTIVE``. Locks expirados (TTL vencido) são
    rompidos automaticamente.

    Não substitui a detecção otimista de conflitos do Delta — há janela de
    corrida entre MERGE e read-back. Use só para reduzir colisões previsíveis;
    o Delta continua sendo a fonte de verdade transacional.

    Raises:
        RuntimeError: se outro ``run_id`` venceu a corrida.
    """
    spark.sql(f"""
        MERGE INTO {qt(tables['locks'])} t
        USING (
            SELECT
                {sql_lit(target)} AS target_table,
                {sql_lit(run_id)} AS run_id,
                {sql_lit(owner)} AS owner,
                current_timestamp() AS acquired_at_utc,
                current_timestamp() + INTERVAL {int(ttl_minutes)} MINUTES AS expires_at_utc,
                {sql_int(ttl_minutes)} AS ttl_minutes,
                CAST(NULL AS TIMESTAMP) AS released_at_utc,
                'ACTIVE' AS status
        ) s
        ON t.target_table = s.target_table
        WHEN MATCHED AND (t.status <> 'ACTIVE' OR t.expires_at_utc < current_timestamp()) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    row = (
        spark.read.table(tables["locks"])
        .where(F.col("target_table") == target)
        .select("run_id", "owner", "status", "acquired_at_utc", "expires_at_utc", "ttl_minutes")
        .first()
    )
    if row is None or row[0] != run_id or row[2] != "ACTIVE":
        details = None if row is None else row.asDict(recursive=True)
        raise RuntimeError(
            f"Lock ocupado para {target}. Este run_id={run_id} não adquiriu o lock. "
            f"Lock atual: {details}"
        )


def release_lock(tables: Dict[str, str], target: str, run_id: str) -> None:
    """Marca o lock como ``RELEASED``. Falhas só logam (nunca propagam)."""
    try:
        spark.sql(f"""
            UPDATE {qt(tables['locks'])}
            SET status = 'RELEASED',
                released_at_utc = current_timestamp()
            WHERE target_table = {sql_lit(target)} AND run_id = {sql_lit(run_id)}
        """)
    except Exception as exc:
        logger.warning(f"Falha ao liberar lock de {target}: {exc}")


def with_retry(
    fn,
    attempts: int = CONFIG.default_retry_attempts,
    backoff_seconds: int = CONFIG.default_retry_backoff_seconds,
):
    """Executa ``fn()`` com retry para conflitos de concorrência Delta.

    Só retenta erros cuja mensagem contenha ``CONCURRENT``, ``CONFLICT``,
    ``RETRY`` ou ``DELTA_CONCURRENT``. Outros erros (OOM, schema mismatch,
    permissão) propagam imediatamente. Backoff linear + jitter.
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            text = str(exc).upper()
            retryable = any(
                token in text for token in ["CONCURRENT", "CONFLICT", "RETRY", "DELTA_CONCURRENT"]
            )
            if not retryable or attempt == attempts:
                raise
            sleep_seconds = backoff_seconds * attempt + random.random()
            logger.warning(
                f"Tentativa {attempt}/{attempts} falhou com erro concorrente. "
                f"Nova tentativa em {sleep_seconds:.1f}s."
            )
            time.sleep(sleep_seconds)
    raise last_exc  # type: ignore
