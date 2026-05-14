"""Orquestrador principal: ``ingest`` e ``ingest_plan``.

Composição mínima — toda a lógica está em submódulos. Aqui fica só o fluxo:
preparar DataFrame, validar, avaliar qualidade, escrever, registrar runs/state,
emitir lineage.
"""
from __future__ import annotations

import logging
import traceback
from datetime import datetime
from dataclasses import asdict, is_dataclass, replace
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import CONFIG, CONTROL_COLUMNS, CTRL_SCHEMA_VERSION, FRAMEWORK_VERSION, FrameworkConfig  # noqa: F401
from .governance import (
    access_sql_preview,
    annotation_sql_preview,
    apply_access_contract,
    apply_annotations_contract,
    record_operations_contract,
    validate_governance_contract,
)
from .lineage import capture_explain, write_explain_plan, write_openlineage_event
from .plan import (  # noqa: F401
    ConnectorSpec,
    IngestionPlan,
    QualityExpression,
    QualityRules,
    SourceSpec,
    build_plan_from_kwargs,
    target_full_table_name,
    target_schema_name,
    validate_plan_shape,
)
from .quality import evaluate_quality, is_abort_only_failure, write_quality_results, write_quarantine
from .schema import (
    build_custom_keys,
    deduplicate_by_order,
    fix_encoding,
    sync_delta_schema,
    table_exists,
    validate_schema_policy,
)
from .shape import apply_shape
from ._spark import runtime_info, safe_cache, safe_unpersist, spark
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
    find_idempotent_run,
    find_idempotent_stream,
    log_error,
    log_access_entries,
    log_annotation_entries,
    log_operations_contract,
    log_run,
    log_schema_changes,
    log_stream_finish,
    log_stream_start,
    release_lock,
    stream_child_run_metrics,
    upsert_state,
    with_retry,
)
from .sources import resolve_batch_source, get_source_resolver
from .watermark import apply_watermark, compute_watermark, get_watermark
from .writers import (
    affected_partition_values,
    delta_version,
    execute_write_mode,
    latest_operation_metrics,
    resolve_write_metrics,
    run_optimize,
    write_strategy,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lakehouse_ingestion")


def _short_error_message(error: Optional[str]) -> Optional[str]:
    """Extrai uma mensagem curta do traceback para ``ctrl_ingestion_runs``."""
    if not error:
        return None
    lines = [line.strip() for line in error.splitlines() if line.strip()]
    return safe_truncate(lines[-1] if lines else error, 2000)


def _lock_owner(plan: IngestionPlan) -> str:
    """Identifica o dono operacional do lock para diagnóstico."""
    return plan.master_run_id or plan.run_group_id or plan.parent_run_id or plan.notebook_name


def _contract_metadata(plan: IngestionPlan) -> Dict[str, Any]:
    """Metadados declarativos do contrato de ingestão."""
    return {
        "description": plan.description,
        "owner": plan.owner,
        "domain": plan.domain,
        "tags": plan.tags,
        "sla": plan.sla,
        "runtime_parameters": plan.runtime_parameters,
        "operations": plan.operations,
        "applied_presets": plan.applied_presets,
        "target_schema": target_schema_name(plan),
    }


def _governance_preview(plan: IngestionPlan, target: str) -> Dict[str, Any]:
    """SQL/acoes previstas para contratos de governanca em dry-run."""
    return {
        "annotations_sql": annotation_sql_preview(target, plan.annotations),
        "access_sql": access_sql_preview(target, plan.access),
        "operations_configured": plan.operations is not None,
        "access_configured": plan.access is not None,
    }


def _annotations_preview(plan: IngestionPlan, target: str) -> Dict[str, Any]:
    """Preview estruturado de annotations para ``dry_run``."""
    if not plan.annotations:
        return {"configured": False, "sql_preview": []}
    def render_optional_dataclass(value: Any) -> Any:
        return asdict(value) if value is not None and is_dataclass(value) else value

    return {
        "configured": True,
        "policy": plan.annotations.policy,
        "table": {
            "description": plan.annotations.table.description,
            "aliases": plan.annotations.table.aliases,
            "tags": plan.annotations.table.tags,
        },
        "columns": {
            column: {
                "description": annotation.description,
                "aliases": annotation.aliases,
                "tags": annotation.tags,
                "pii": render_optional_dataclass(annotation.pii),
                "deprecated": render_optional_dataclass(annotation.deprecated),
            }
            for column, annotation in plan.annotations.columns.items()
        },
        "sql_preview": annotation_sql_preview(target, plan.annotations),
    }


def _skip_result(
    plan: IngestionPlan,
    run_id: str,
    target: str,
    source_name: str,
    source_metadata: Dict[str, Any],
    metrics_source: str,
    runtime_meta: Dict[str, Optional[str]],
    skip_reason: str,
    skipped_by_run_id: Optional[str],
    stage_durations: Dict[str, float],
) -> Dict[str, Any]:
    """Payload padronizado para execuções puladas por idempotência."""
    return {
        "status": "SKIPPED",
        "run_id": run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "source_table": source_name,
        "source": source_metadata,
        "mode": plan.mode,
        "applied_presets": plan.applied_presets,
        "rows_read": 0,
        "rows_written": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_quarantined": 0,
        "watermark_previous": None,
        "watermark_current": None,
        "quality_status": "SKIPPED",
        "schema_changes": {},
        "operation_metrics": {},
        "metrics_source": metrics_source,
        "stage_durations": stage_durations,
        "write_committed": False,
        "delta_version_before": None,
        "delta_version_after": None,
        "write_delta_version": None,
        "explain_captured": False,
        "openlineage_event_emitted": False,
        "openlineage_event": None,
        "error_message": None,
        "idempotency_key": plan.idempotency_key,
        "idempotency_policy": plan.idempotency_policy,
        "skip_reason": skip_reason,
        "skipped_by_run_id": skipped_by_run_id,
        "contract_metadata": _contract_metadata(plan),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        **runtime_meta,
    }


def _source_metadata_for_legacy_source(source_name: str, source_type: str) -> Dict[str, Any]:
    return {
        "source_type": source_type,
        "source_connector": source_type,
        "source_name": source_name,
        "source_provider": None,
        "source_format": None,
        "source_path": None,
        "source_options_redacted": {},
        "source_read_redacted": {},
        "source_request_redacted": {},
        "source_auth_redacted": {},
        "source_pagination_redacted": {},
        "source_response_redacted": {},
        "source_incremental_redacted": {},
        "source_limits_redacted": {},
        "source_capabilities": {},
        "source_metrics": {"read_strategy": source_type},
    }


def _source_is_complete(plan: IngestionPlan) -> bool:
    if isinstance(plan.source, ConnectorSpec):
        return bool(
            plan.source.read.get("source_complete", False)
            or plan.source.read.get("full_snapshot", False)
        )
    return False


def _plan_with_connector_runtime(plan: IngestionPlan, wm_prev: Optional[str]) -> IngestionPlan:
    if not isinstance(plan.source, ConnectorSpec):
        return plan
    runtime_parameters = dict(plan.runtime_parameters or {})
    runtime_parameters["_contractforge_watermark_previous"] = wm_prev
    runtime_parameters["_contractforge_target_table"] = target_full_table_name(plan)
    return replace(plan, runtime_parameters=runtime_parameters)


def _resolve_source(plan: IngestionPlan) -> Tuple[DataFrame, str, Dict[str, Any]]:
    """Resolve ``plan.source`` em ``(DataFrame, nome_para_log, metadados)``.

    String é interpretada como nome de tabela; se não tiver pontos, é
    qualificada com ``catalog.<target_schema ou layer>.<source>``. DataFrames
    passam direto e recebem o rótulo ``"dataframe"`` no log.
    """
    if isinstance(plan.source, str):
        source_full = (
            plan.source
            if "." in plan.source
            else full_table_name(plan.catalog, target_schema_name(plan), plan.source)
        )
        return spark.read.table(source_full), source_full, _source_metadata_for_legacy_source(source_full, "table")
    if isinstance(plan.source, ConnectorSpec):
        resolved = resolve_batch_source(plan.source, plan)
        return resolved.df, resolved.label, resolved.metadata
    return plan.source, "dataframe", _source_metadata_for_legacy_source("dataframe", "dataframe")


def _autoloader_connector_to_source_spec(source: ConnectorSpec) -> SourceSpec:
    path = str(source.path or "").strip()
    schema_location = str(source.read.get("schema_location") or source.options.get("cloudFiles.schemaLocation") or "").strip()
    checkpoint_location = str(source.read.get("checkpoint_location") or "").strip()
    if not path:
        raise ValueError("source.path é obrigatório para connector=autoloader")
    if not schema_location:
        raise ValueError("source.read.schema_location é obrigatório para connector=autoloader")
    if not checkpoint_location:
        raise ValueError("source.read.checkpoint_location é obrigatório para connector=autoloader")
    return SourceSpec(
        type="autoloader",
        path=path,
        format=str(source.format or "parquet"),
        schema_location=schema_location,
        checkpoint_location=checkpoint_location,
        trigger="available_now",
        options=source.options,
        schema_hints=source.read.get("schema_hints"),
        include_existing_files=bool(source.read.get("include_existing_files", True)),
        max_files_per_trigger=source.read.get("max_files_per_trigger"),
    )


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


def _prepare_dataframe(
    df: DataFrame,
    plan: IngestionPlan,
    run_id: str,
    run_date: str,
    run_ts: str,
    wm_prev: Optional[str],
) -> DataFrame:
    """Aplica todas as transformações pré-quality em ordem determinística.

    Sequência: ``select_columns`` -> ``column_mapping`` -> ``shape`` ->
    ``filter_expression`` -> ``custom_keys`` -> ``apply_watermark`` ->
    ``deduplicate_by_order`` -> ``fix_encoding`` -> adição das colunas de
    controle (``ingestion_date``, ``source_system``, ``__run_id``).
    """
    if plan.select_columns:
        validate_cols(df, plan.select_columns, "select_columns")
        df = df.select(*plan.select_columns)
    if plan.column_mapping:
        _validate_column_mapping(df, plan)
        for source_col, target_col in plan.column_mapping.items():
            df = df.withColumnRenamed(source_col, target_col)
    if plan.shape:
        df = apply_shape(df, plan.shape, layer=plan.layer)
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
    _validate_no_reserved_source_columns(df)
    df = (
        df.withColumn("ingestion_date", F.to_date(F.lit(run_date)))
        .withColumn("ingestion_ts_utc", F.lit(run_ts).cast("timestamp"))
        .withColumn("source_system", F.lit(plan.source_system))
        .withColumn("__run_id", F.lit(run_id))
    )
    return df


def _validate_column_mapping(df: DataFrame, plan: IngestionPlan) -> None:
    """Valida renomeação declarativa source -> target antes de aplicar mapping."""
    validate_cols(df, list(plan.column_mapping.keys()), "column_mapping")
    existing = set(df.columns)
    targets = list(plan.column_mapping.values())
    duplicate_targets = sorted({target for target in targets if targets.count(target) > 1})
    if duplicate_targets:
        raise ValueError(f"column_mapping possui destinos duplicados: {duplicate_targets}")
    reserved_targets = sorted(set(targets) & CONTROL_COLUMNS)
    if reserved_targets:
        raise ValueError(f"column_mapping não pode produzir colunas técnicas reservadas: {reserved_targets}")
    collisions = sorted(
        target
        for source, target in plan.column_mapping.items()
        if target in existing and target != source
    )
    if collisions:
        raise ValueError(f"column_mapping produziria colisão com colunas existentes: {collisions}")


def _validate_no_reserved_source_columns(df: DataFrame) -> None:
    """Impede sobrescrita silenciosa de colunas técnicas vindas da origem."""
    reserved = sorted(set(df.columns) & CONTROL_COLUMNS)
    if reserved:
        raise ValueError(
            "Origem contém colunas técnicas reservadas pelo framework. "
            f"Renomeie/remova antes da ingestão ou use column_mapping: {reserved}"
        )


def _validate_merge_key_nulls(df: DataFrame, keys: list[str], row_count: int, mode: str) -> None:
    """Protege MERGE contra chaves naturais totalmente nulas."""
    if not keys or row_count <= 0:
        return
    null_exprs = [
        F.sum(F.col(key).isNull().cast("long")).alias(f"__nulls_{idx}")
        for idx, key in enumerate(keys)
    ]
    all_keys_null = None
    for key in keys:
        condition = F.col(key).isNull()
        all_keys_null = condition if all_keys_null is None else (all_keys_null & condition)
    row = df.agg(
        *null_exprs,
        F.sum(all_keys_null.cast("long")).alias("__all_keys_null"),
    ).first()
    if row is None:
        return
    per_key_nulls = {key: int(row[f"__nulls_{idx}"] or 0) for idx, key in enumerate(keys)}
    all_null_count = int(row["__all_keys_null"] or 0)
    if all_null_count == row_count:
        raise ValueError(
            f"mode={mode} recebeu {row_count} linhas com merge_keys totalmente nulas. "
            f"keys={keys}. Corrija a origem ou adicione quality_rules.not_null."
        )
    nullable_keys = {key: count for key, count in per_key_nulls.items() if count > 0}
    if nullable_keys:
        logger.warning(
            "merge_keys contém valores nulos; revise quality_rules.not_null para evitar matches inesperados. "
            f"mode={mode}, null_counts={nullable_keys}, rows={row_count}"
        )


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
    _validate_static_plan_options(plan)

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

    schema_changes = validate_schema_policy(
        df,
        target,
        plan.schema_policy,
        allow_type_widening=plan.allow_type_widening,
    )
    if apply_changes:
        sync_delta_schema(df, target, schema_changes, plan.schema_policy)
    return schema_changes


def _validate_static_plan_options(plan: IngestionPlan) -> None:
    """Valida combinações perigosas do plano sem tocar no DataFrame ou target."""
    validate_plan_shape(plan)
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
    if plan.partition_value and not plan.partition_column:
        raise ValueError("partition_value requer partition_column")
    if plan.zorder_columns and not plan.optimize_after_write:
        logger.warning("zorder_columns foi informado, mas optimize_after_write=false; ZORDER não será executado.")
    if plan.mode == "scd1_upsert" and plan.merge_strategy in {"delta_by_partition", "replace_partitions"}:
        if not plan.merge_partition_column:
            raise ValueError(f"merge_strategy={plan.merge_strategy} requer merge_partition_column")
        if (
            plan.merge_strategy == "replace_partitions"
            and plan.partition_column
            and plan.partition_column != plan.merge_partition_column
        ):
            raise ValueError(
                "merge_strategy=replace_partitions requer partition_column igual a "
                "merge_partition_column quando ambos forem informados"
            )
    if plan.mode == "scd1_upsert" and plan.merge_strategy == "replace_partitions":
        if not plan.replace_partitions_source_complete and not _source_is_complete(plan):
            raise ValueError(
                "merge_strategy=replace_partitions exige replace_partitions_source_complete=True "
                "ou source.read.source_complete=true para confirmar que o source contém "
                "o estado completo das partições afetadas"
            )
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
        if isinstance(plan.source, ConnectorSpec) and not _source_is_complete(plan):
            raise ValueError(
                "snapshot_soft_delete com source connector exige source.read.source_complete=true "
                "ou source.read.full_snapshot=true."
            )


def _build_dry_run_result(
    plan: IngestionPlan,
    run_id: str,
    target: str,
    source_name: str,
    source_metadata: Dict[str, Any],
    rows_read: int,
    rows_quarantined: int,
    wm_prev: Optional[str],
    wm_candidate: Optional[str],
    quality_status: str,
    schema_changes: Dict[str, Any],
    started_dt: datetime,
    df: DataFrame,
    runtime_meta: Dict[str, Optional[str]],
    stage_durations: Dict[str, float],
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
        "source": source_metadata,
        "mode": plan.mode,
        "target_schema": target_schema_name(plan),
        "applied_presets": plan.applied_presets,
        "write_strategy": write_strategy(plan.mode),
        "rows_read": rows_read,
        "rows_effective": rows_read - rows_quarantined,
        "rows_written": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_quarantined": rows_quarantined,
        "affected_partitions": affected_partition_values(
            df, plan.partition_column or plan.merge_partition_column
        ),
        "watermark_previous": wm_prev,
        "watermark_candidate": wm_candidate,
        "quality_status": quality_status,
        "schema_changes": schema_changes,
        "metrics_source": "logical",
        "stage_durations": stage_durations,
        "duration_seconds": (finished_dt - started_dt).total_seconds(),
        "explain_captured": plan.explain_mode,
        "openlineage_enabled": plan.openlineage_enabled,
        "idempotency_key": plan.idempotency_key,
        "idempotency_policy": plan.idempotency_policy,
        "contract_metadata": _contract_metadata(plan),
        "governance": _governance_preview(plan, target),
        "annotations_preview": _annotations_preview(plan, target),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        **runtime_meta,
    }


def _finalize_execution(
    tables: Dict[str, str],
    plan: IngestionPlan,
    run_id: str,
    run_ts: str,
    run_date: str,
    source_name: str,
    source_metadata: Dict[str, Any],
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
    metrics_source: str,
    runtime_meta: Dict[str, Optional[str]],
    skip_reason: Optional[str],
    skipped_by_run_id: Optional[str],
    stage_durations: Dict[str, float],
    governance_results: Optional[Dict[str, Any]],
) -> None:
    """Monta o payload completo e grava em ``ctrl_ingestion_runs`` via ``log_run``.

    Chamado no ``finally`` do orquestrador — sempre executa, mesmo em falha.
    """
    duration = (finished_dt - started_dt).total_seconds()
    annotations_result = (governance_results or {}).get("annotations") or {}
    operations_result = (governance_results or {}).get("operations") or {}
    log_run(
        tables,
        {
            "run_id": run_id,
            "run_ts_utc": run_ts,
            "run_date": run_date,
            "notebook_name": plan.notebook_name,
            "layer": plan.layer,
            "source_table": source_name,
            "source_type": source_metadata.get("source_type"),
            "source_connector": source_metadata.get("source_connector"),
            "source_name": source_metadata.get("source_name"),
            "source_provider": source_metadata.get("source_provider"),
            "source_format": source_metadata.get("source_format"),
            "source_path": source_metadata.get("source_path"),
            "source_options_json": to_json(source_metadata.get("source_options_redacted")),
            "source_read_json": to_json(source_metadata.get("source_read_redacted")),
            "source_request_json": to_json(source_metadata.get("source_request_redacted")),
            "source_auth_json": to_json(source_metadata.get("source_auth_redacted")),
            "source_pagination_json": to_json(source_metadata.get("source_pagination_redacted")),
            "source_response_json": to_json(source_metadata.get("source_response_redacted")),
            "source_incremental_json": to_json(source_metadata.get("source_incremental_redacted")),
            "source_limits_json": to_json(source_metadata.get("source_limits_redacted")),
            "source_capabilities_json": to_json(source_metadata.get("source_capabilities")),
            "source_metrics_json": to_json(source_metadata.get("source_metrics")),
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
            "stage_durations_json": to_json(stage_durations),
            "contract_description": plan.description,
            "contract_owner": plan.owner,
            "contract_domain": plan.domain,
            "contract_tags_json": to_json(plan.tags),
            "contract_sla": plan.sla,
            "runtime_parameters_json": to_json(plan.runtime_parameters),
            "operation_metrics_json": to_json(operation_metrics),
            "write_started_at_utc": write_started_at,
            "write_finished_at_utc": write_finished_at,
            "delta_version_before": delta_version_before,
            "delta_version_after": delta_version_after,
            "write_committed": write_committed,
            "error_message": _short_error_message(error),
            "parent_run_id": plan.parent_run_id,
            "run_group_id": plan.run_group_id,
            "master_job_id": plan.master_job_id,
            "master_run_id": plan.master_run_id,
            "idempotency_key": plan.idempotency_key,
            "idempotency_policy": plan.idempotency_policy,
            "skip_reason": skip_reason,
            "skipped_by_run_id": skipped_by_run_id,
            "metrics_source": metrics_source,
            "framework_version": FRAMEWORK_VERSION,
            "ctrl_schema_version": CTRL_SCHEMA_VERSION,
            "annotations_status": annotations_result.get("status"),
            "annotations_result_json": to_json(annotations_result) if annotations_result else None,
            "ownership_json": to_json(operations_result.get("ownership")) if operations_result else None,
            "operations_json": to_json(operations_result.get("operations")) if operations_result else None,
            **runtime_meta,
        },
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
        "status": status,
        "stream_run_id": stream_run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "source_table": source_name,
        "mode": plan.mode,
        "applied_presets": plan.applied_presets,
        "batches_processed": metrics["batches_processed"],
        "total_rows_read": metrics["total_rows_read"],
        "total_rows_written": metrics["total_rows_written"],
        "total_rows_quarantined": metrics["total_rows_quarantined"],
        "batch_results": batch_results,
        "stage_durations": stage_durations,
        "duration_seconds": (finished_dt - started_dt).total_seconds(),
        "error_message": _short_error_message(error),
        "idempotency_key": plan.idempotency_key,
        "idempotency_policy": plan.idempotency_policy,
        "skip_reason": skip_reason,
        "skipped_by_stream_run_id": skipped_by_stream_run_id,
        "contract_metadata": _contract_metadata(plan),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        **runtime_meta,
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
            log_stream_start(
                tables,
                {
                    "stream_run_id": stream_run_id,
                    "idempotency_key": plan.idempotency_key,
                    "idempotency_policy": plan.idempotency_policy,
                    "skip_reason": skip_reason,
                    "skipped_by_stream_run_id": skipped_by_stream_run_id,
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
                    "ended_at_utc": utc_now_str(),
                    "duration_seconds": (utc_now_ts() - started_dt).total_seconds(),
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
                },
            )
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

        log_stream_start(
            tables,
            {
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
                "status": "RUNNING",
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
            },
        )
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
        logger.error(f"Stream de ingestão falhou: {exc}")
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
                        {
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
                        },
                    )
                    stream_logged = True
                except Exception as start_log_exc:
                    logger.error(f"Falha ao registrar início do stream: {start_log_exc}")
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
                logger.error(f"Falha ao registrar stream: {log_exc}")
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
                    logger.error(f"Falha ao registrar erro completo do stream: {error_log_exc}")

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
    if isinstance(plan.source, ConnectorSpec) and plan.source.connector == "autoloader":
        return ingest_stream_plan(replace(plan, source=_autoloader_connector_to_source_spec(plan.source)))
    if isinstance(plan.source, SourceSpec):
        return ingest_stream_plan(plan)

    run_id = new_run_id()
    run_ts = utc_now_str()
    run_date = today_str()
    started_dt = utc_now_ts()
    target = target_full_table_name(plan)
    runtime_meta = runtime_info()
    stage_durations: Dict[str, float] = {}
    if plan.dry_run:
        tables = ctrl_table_names(plan.catalog, plan.ctrl_schema)
    else:
        stage_started = utc_now_ts()
        tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
        stage_durations["control_setup"] = (utc_now_ts() - stage_started).total_seconds()

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
    metrics_source = "logical"
    governance_results: Dict[str, Any] = {}
    explain_text: Optional[str] = None
    openlineage_event: Optional[Dict[str, Any]] = None
    write_started_at: Optional[str] = None
    write_finished_at: Optional[str] = None
    write_committed = False
    delta_version_before: Optional[int] = None
    delta_version_after: Optional[int] = None
    wm_candidate: Optional[str] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    skip_reason: Optional[str] = None
    skipped_by_run_id: Optional[str] = None
    prepared_df: Optional[DataFrame] = None
    row_metrics: Dict[str, int] = {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}
    source_metadata = _source_metadata_for_legacy_source(source_name, "unknown")

    try:
        stage_started = utc_now_ts()
        previous_success = find_idempotent_run(
            tables, target, plan.idempotency_key, status="SUCCESS"
        )
        stage_durations["idempotency"] = (utc_now_ts() - stage_started).total_seconds()
        previous_status = previous_success.get("status") if previous_success else None
        previous_run_id = previous_success.get("run_id") if previous_success else None
        if (
            plan.idempotency_policy in {"skip_if_success", "rerun_if_failed"}
            and previous_status == "SUCCESS"
        ):
            status = "SKIPPED"
            quality_status = "SKIPPED"
            skip_reason = "idempotency_key_already_succeeded"
            skipped_by_run_id = previous_run_id
            return _skip_result(
                plan, run_id, target, source_name, source_metadata, metrics_source, runtime_meta,
                skip_reason, skipped_by_run_id, stage_durations,
            )
        if plan.idempotency_policy == "fail_if_success" and previous_status == "SUCCESS":
            raise RuntimeError(
                "idempotency_policy=fail_if_success bloqueou a execução: "
                f"idempotency_key={plan.idempotency_key!r} já teve sucesso em run_id={previous_run_id}"
            )

        if plan.lock_enabled and not plan.dry_run:
            stage_started = utc_now_ts()
            acquire_lock(tables, target, run_id, owner=_lock_owner(plan))
            stage_durations["lock_acquire"] = (utc_now_ts() - stage_started).total_seconds()

        if plan.hooks and plan.hooks.before_read:
            stage_started = utc_now_ts()
            plan.hooks.before_read(plan)
            stage_durations["hook_before_read"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        wm_prev = (
            get_watermark(tables["state"], target, plan.watermark_columns)
            if plan.watermark_columns
            else None
        )
        raw_df, source_name, source_metadata = _resolve_source(_plan_with_connector_runtime(plan, wm_prev))
        stage_durations["read"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        ingestion_ts = started_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        prepared_df = _prepare_dataframe(raw_df, plan, run_id, run_date, ingestion_ts, wm_prev)
        if plan.hooks and plan.hooks.after_prepare:
            prepared_df = plan.hooks.after_prepare(prepared_df, plan)
            if not isinstance(prepared_df, DataFrame):
                raise ValueError("hooks.after_prepare deve retornar um DataFrame")
        prepared_df = safe_cache(prepared_df, plan.use_cache)
        stage_durations["prepare"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        schema_changes = _validate_plan(plan, prepared_df, target, apply_changes=not plan.dry_run)
        if not plan.dry_run:
            log_schema_changes(tables, run_id, target, schema_changes)
        stage_durations["schema"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        rows_read = prepared_df.count()
        wm_candidate = (
            compute_watermark(prepared_df, plan.watermark_columns)
            if plan.watermark_columns and rows_read > 0
            else wm_prev
        )
        stage_durations["watermark"] = (utc_now_ts() - stage_started).total_seconds()

        if plan.explain_mode:
            stage_started = utc_now_ts()
            explain_text = capture_explain(prepared_df, plan.explain_format)
            if not plan.dry_run:
                write_explain_plan(
                    tables, run_id, target, source_name, plan.mode, plan.explain_format, explain_text
                )
            stage_durations["explain"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        quality_status, quality_results, valid_df, quarantined_df, rows_quarantined = (
            evaluate_quality(prepared_df, plan.quality_rules, run_id, target)
        )
        if not plan.dry_run:
            write_quality_results(tables, run_id, target, quality_results)
        stage_durations["quality"] = (utc_now_ts() - stage_started).total_seconds()

        if quality_status in {"FAILED", "WARNED"}:
            effective_action = plan.on_quality_fail
            actionable_failed = [r for r in quality_results if r.get("severity") != "warn"]
            abort_only_failed = [
                r
                for r in actionable_failed
                if r.get("severity") == "abort" or is_abort_only_failure(r["rule_name"])
            ]
            if not actionable_failed:
                logger.warning(
                    f"Quality gates emitiram warnings; execução continuará: {to_json(quality_results)}"
                )
                effective_action = "warn"
            if effective_action == "quarantine" and abort_only_failed:
                names = sorted({r["rule_name"] for r in abort_only_failed})
                logger.warning(
                    f"Regras abortivas {names} não são quarentenáveis em nível de linha. "
                    "Escalando on_quality_fail de 'quarantine' para 'fail'."
                )
                effective_action = "fail"

            if effective_action == "fail":
                raise ValueError(f"Quality gates falharam: {to_json(actionable_failed)}")
            if effective_action == "quarantine":
                if not plan.dry_run:
                    write_quarantine(
                        tables, quarantined_df, run_id, target, "quality_gate", to_json(actionable_failed)
                    )
                prepared_df = valid_df
            elif effective_action == "warn":
                logger.warning(
                    f"Quality gates falharam, mas execução continuará: {to_json(quality_results)}"
                )

        effective_rows = (
            rows_read - rows_quarantined
            if quality_status == "FAILED" and plan.on_quality_fail == "quarantine"
            else rows_read
        )
        if plan.mode in {"scd1_upsert", "snapshot_soft_delete", "scd2_historical"}:
            _validate_merge_key_nulls(prepared_df, plan.merge_keys, effective_rows, plan.mode)

        if plan.dry_run:
            return _build_dry_run_result(
                plan, run_id, target, source_name, source_metadata, rows_read, rows_quarantined, wm_prev,
                wm_candidate, quality_status, schema_changes, started_dt, prepared_df, runtime_meta,
                stage_durations,
            )

        delta_version_before = delta_version(target) if table_exists(target) else None
        stage_started = utc_now_ts()
        write_started_at = utc_now_str()
        if plan.hooks and plan.hooks.before_write:
            prepared_df = plan.hooks.before_write(prepared_df, plan)
            if not isinstance(prepared_df, DataFrame):
                raise ValueError("hooks.before_write deve retornar um DataFrame")
            effective_rows = prepared_df.count()
        rows_written = with_retry(
            lambda: execute_write_mode(plan, prepared_df, target, effective_rows),
            attempts=plan.retry_attempts or CONFIG.default_retry_attempts,
            backoff_seconds=(
                CONFIG.default_retry_backoff_seconds
                if plan.retry_backoff_seconds is None
                else plan.retry_backoff_seconds
            ),
        )
        write_finished_at = utc_now_str()
        delta_version_after = delta_version(target) if table_exists(target) else None
        write_committed = rows_written > 0 and delta_version_after != delta_version_before
        stage_durations["write"] = (utc_now_ts() - stage_started).total_seconds()

        if rows_written > 0 and plan.optimize_after_write:
            stage_started = utc_now_ts()
            run_optimize(target, plan.zorder_columns)
            stage_durations["optimize"] = (utc_now_ts() - stage_started).total_seconds()

        stage_started = utc_now_ts()
        wm_current = (
            compute_watermark(prepared_df, plan.watermark_columns)
            if plan.watermark_columns and rows_read > 0
            else wm_prev
        )
        delta_metrics = latest_operation_metrics(target) if table_exists(target) else {}
        row_metrics, operation_metrics, metrics_source = resolve_write_metrics(
            plan, rows_written, delta_metrics
        )
        stage_started = utc_now_ts()
        governance_validation = validate_governance_contract(target, plan.annotations, None)
        if governance_validation["status"] == "FAILED":
            raise ValueError(f"Contrato de governança inválido: {to_json(governance_validation['issues'])}")
        governance_results = {
            "validation": governance_validation,
            "operations": record_operations_contract(
                tables,
                run_id,
                target,
                plan.operations,
                log_operations_contract,
            ),
            "annotations": apply_annotations_contract(
                tables,
                run_id,
                target,
                plan.annotations,
                log_annotation_entries,
            ),
            "access": {
                "status": "DEFERRED",
                "reason": "access deve ser aplicado pelo fluxo dedicado de governanca",
                "sql_preview": access_sql_preview(target, plan.access),
            } if plan.access else {"status": "NOT_CONFIGURED"},
        }
        stage_durations["governance"] = (utc_now_ts() - stage_started).total_seconds()
        if plan.hooks and plan.hooks.after_write:
            plan.hooks.after_write(
                {
                    "run_id": run_id,
                    "target_table": target,
                    "rows_written": rows_written,
                    "operation_metrics": operation_metrics,
                    "metrics_source": metrics_source,
                },
                plan,
            )
        delta_version_after = operation_metrics.get("version", delta_version_after)
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
        stage_durations["state_update"] = (utc_now_ts() - stage_started).total_seconds()

    except Exception as exc:
        status = "FAILED"
        error_type = type(exc).__name__
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
                stage_started = utc_now_ts()
                output_df = spark.read.table(target) if table_exists(target) else None
                if status != "SKIPPED":
                    openlineage_event = write_openlineage_event(
                        tables, plan, run_id, target, source_name, status, started_dt, finished_dt,
                        prepared_df, output_df, rows_read, rows_written, delta_version_before,
                        delta_version_after, operation_metrics,
                    )
                stage_durations["lineage"] = (utc_now_ts() - stage_started).total_seconds()
            except Exception as lineage_exc:
                logger.error(f"Falha ao registrar evento OpenLineage: {lineage_exc}")
            try:
                _finalize_execution(
                    tables, plan, run_id, run_ts, run_date, source_name, source_metadata, target,
                    status, started_dt, finished_dt, rows_read, rows_written, rows_quarantined,
                    wm_prev, wm_current, quality_status, schema_changes, operation_metrics, write_started_at,
                    write_finished_at, delta_version_before, delta_version_after, write_committed,
                    error, row_metrics, metrics_source, runtime_meta,
                    skip_reason, skipped_by_run_id, stage_durations, governance_results,
                )
            except Exception as log_exc:
                logger.error(f"Falha ao registrar execução: {log_exc}")
            if error:
                try:
                    log_error(
                        tables,
                        {
                            "run_id": run_id,
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
                    logger.error(f"Falha ao registrar erro completo: {error_log_exc}")

    return {
        "status": status,
        "run_id": run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "source_table": source_name,
        "source": source_metadata,
        "mode": plan.mode,
        "applied_presets": plan.applied_presets,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_inserted": row_metrics.get("rows_inserted", 0),
        "rows_updated": row_metrics.get("rows_updated", 0),
        "rows_deleted": row_metrics.get("rows_deleted", 0),
        "rows_quarantined": rows_quarantined,
        "watermark_previous": wm_prev,
        "watermark_current": wm_current,
        "quality_status": quality_status,
        "schema_changes": schema_changes,
        "operation_metrics": operation_metrics,
        "metrics_source": metrics_source,
        "stage_durations": stage_durations,
        "write_committed": write_committed,
        "delta_version_before": delta_version_before,
        "delta_version_after": delta_version_after,
        "write_delta_version": delta_version_after if write_committed else None,
        "explain_captured": bool(explain_text),
        "openlineage_event_emitted": bool(openlineage_event),
        "openlineage_event": openlineage_event,
        "error_message": _short_error_message(error),
        "idempotency_key": plan.idempotency_key,
        "idempotency_policy": plan.idempotency_policy,
        "skip_reason": skip_reason,
        "skipped_by_run_id": skipped_by_run_id,
        "contract_metadata": _contract_metadata(plan),
        "governance": governance_results,
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
        **runtime_meta,
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


def ingest_bundle(path: str) -> Dict[str, Any]:
    """Carrega contrato dividido e executa o plano de ingestao."""
    from .contract_bundle import load_contract_bundle

    bundle = load_contract_bundle(path)
    return ingest_plan(bundle.ingestion)


def apply_governance_bundle(
    path: str,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Aplica operations e annotations de um bundle sem reprocessar dados.

    Access tem ciclo proprio por normalmente exigir permissoes elevadas. Use
    ``apply_access_bundle`` para grants, row filters e column masks.
    """
    from .contract_bundle import governance_preview, load_contract_bundle

    bundle = load_contract_bundle(path)
    plan = bundle.ingestion
    target = target_full_table_name(plan)
    governance_run_id = run_id or new_run_id()
    tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
    stage_started = utc_now_ts()
    governance_validation = validate_governance_contract(target, plan.annotations, None)
    if governance_validation["status"] == "FAILED":
        raise ValueError(f"Contrato de governança inválido: {to_json(governance_validation['issues'])}")
    results = {
        "validation": governance_validation,
        "operations": record_operations_contract(
            tables,
            governance_run_id,
            target,
            plan.operations,
            log_operations_contract,
        ),
        "annotations": apply_annotations_contract(
            tables,
            governance_run_id,
            target,
            plan.annotations,
            log_annotation_entries,
        ),
        "access": {
            "status": "DEFERRED",
            "reason": "access deve ser aplicado por apply_access_bundle",
            "sql_preview": access_sql_preview(target, plan.access),
        } if plan.access else {"status": "NOT_CONFIGURED"},
    }
    return {
        "status": "SUCCESS",
        "run_id": governance_run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "governance": results,
        "preview": governance_preview(bundle),
        "duration_seconds": (utc_now_ts() - stage_started).total_seconds(),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
    }


def apply_annotations_bundle(path: str, run_id: Optional[str] = None) -> Dict[str, Any]:
    """Aplica apenas annotations de um bundle, sem operations nem access."""
    from .contract_bundle import governance_preview, load_contract_bundle

    bundle = load_contract_bundle(path)
    plan = bundle.ingestion
    target = target_full_table_name(plan)
    annotations_run_id = run_id or new_run_id()
    tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
    stage_started = utc_now_ts()
    validation = validate_governance_contract(target, plan.annotations, None)
    if validation["status"] == "FAILED":
        raise ValueError(f"Contrato de annotations inválido: {to_json(validation['issues'])}")
    result = apply_annotations_contract(
        tables,
        annotations_run_id,
        target,
        plan.annotations,
        log_annotation_entries,
    )
    return {
        "status": "SUCCESS" if result.get("status") not in {"FAILED", "WARNED"} else result.get("status"),
        "run_id": annotations_run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "validation": validation,
        "annotations": result,
        "preview": governance_preview(bundle),
        "duration_seconds": (utc_now_ts() - stage_started).total_seconds(),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
    }


def apply_access_bundle(
    path: str,
    run_id: Optional[str] = None,
    *,
    force_revoke: bool = False,
) -> Dict[str, Any]:
    """Aplica apenas o contrato de access de um bundle."""
    from .contract_bundle import governance_check, load_contract_bundle

    bundle = load_contract_bundle(path)
    plan = bundle.ingestion
    target = target_full_table_name(plan)
    access_run_id = run_id or new_run_id()
    tables = ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
    stage_started = utc_now_ts()
    validation = validate_governance_contract(target, None, plan.access)
    if validation["status"] == "FAILED":
        raise ValueError(f"Contrato de access inválido: {to_json(validation['issues'])}")
    result = apply_access_contract(
        tables,
        access_run_id,
        target,
        plan.access,
        log_access_entries,
        allow_revoke_unmanaged=force_revoke,
    )
    return {
        "status": "SUCCESS" if result.get("status") not in {"FAILED", "WARNED"} else result.get("status"),
        "run_id": access_run_id,
        "target_table": target,
        "target_schema": target_schema_name(plan),
        "validation": validation,
        "access": result,
        "check": governance_check(bundle),
        "duration_seconds": (utc_now_ts() - stage_started).total_seconds(),
        "framework_version": FRAMEWORK_VERSION,
        "ctrl_schema_version": CTRL_SCHEMA_VERSION,
    }


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
