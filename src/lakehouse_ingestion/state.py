"""Tabelas de controle, log de runs, upsert de estado, locks e retry."""
from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from .config import CONFIG
from .plan import IngestionPlan
from ._spark import spark
from ._sql import full_table_name, q, qt, safe_truncate, sql_int, sql_lit

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
    }


def ensure_ctrl_tables(catalog: str, schema: str) -> Dict[str, str]:
    """Cria (idempotente) o schema e as 7 tabelas de controle.

    Retorna um dict com nomes lógicos -> nomes qualificados:

    - ``runs`` (particionada por ``run_date``)
    - ``state`` (uma linha por ``target_table``, PK)
    - ``quality`` (uma linha por regra falhada por execução)
    - ``quarantine`` (uma linha por linha quarentenada)
    - ``locks`` (uma linha por ``target_table``, PK)
    - ``explain`` (planos Spark capturados)
    - ``lineage`` (eventos OpenLineage como JSON)

    Não migra schemas existentes — se o framework atualizar, ajuste manualmente.
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
            master_run_id STRING
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
            last_updated_at_utc TIMESTAMP,
            CONSTRAINT pk_state PRIMARY KEY (target_table)
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {qt(tables['quality'])} (
            run_id STRING,
            target_table STRING,
            rule_name STRING,
            status STRING,
            failed_count BIGINT,
            checked_at_utc TIMESTAMP,
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
            acquired_at_utc TIMESTAMP,
            expires_at_utc TIMESTAMP,
            status STRING,
            CONSTRAINT pk_lock PRIMARY KEY (target_table)
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
    return tables


_RUN_COLUMNS = [
    "run_id", "run_ts_utc", "run_date", "notebook_name", "layer", "source_table",
    "target_table", "mode", "status", "rows_read", "rows_written", "rows_inserted",
    "rows_updated", "rows_deleted", "rows_quarantined", "watermark_column",
    "watermark_previous", "watermark_current", "started_at_utc", "finished_at_utc",
    "duration_seconds", "quality_status", "schema_policy", "schema_changes_json",
    "operation_metrics_json", "write_started_at_utc", "write_finished_at_utc",
    "delta_version_before", "delta_version_after", "write_committed", "error_message",
    "parent_run_id", "run_group_id", "master_job_id", "master_run_id",
]
_RUN_INT_COLUMNS = {
    "rows_read", "rows_written", "rows_inserted", "rows_updated", "rows_deleted",
    "rows_quarantined", "delta_version_before", "delta_version_after",
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
                current_timestamp() AS acquired_at_utc,
                current_timestamp() + INTERVAL {int(ttl_minutes)} MINUTES AS expires_at_utc,
                'ACTIVE' AS status
        ) s
        ON t.target_table = s.target_table
        WHEN MATCHED AND (t.status <> 'ACTIVE' OR t.expires_at_utc < current_timestamp()) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    row = (
        spark.read.table(tables["locks"])
        .where(F.col("target_table") == target)
        .select("run_id", "status", "expires_at_utc")
        .first()
    )
    if row is None or row[0] != run_id or row[1] != "ACTIVE":
        raise RuntimeError(f"Não foi possível adquirir lock para {target}. Lock atual: {row}")


def release_lock(tables: Dict[str, str], target: str, run_id: str) -> None:
    """Marca o lock como ``RELEASED``. Falhas só logam (nunca propagam)."""
    try:
        spark.sql(f"""
            UPDATE {qt(tables['locks'])}
            SET status = 'RELEASED'
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
