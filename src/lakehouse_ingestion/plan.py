"""Contratos declarativos: IngestionPlan, QualityRules e construtor a partir de kwargs."""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union

from .config import (
    CONFIG,
    CONTROL_COLUMNS,
    IdempotencyPolicy,
    Layer,
    MergeStrategy,
    QualityFailAction,
    QualityRuleSeverity,
    SchemaPolicy,
    Source,
    VALID_EXPLAIN_FORMATS,
    VALID_FILE_CONNECTOR_FORMATS,
    VALID_IDEMPOTENCY_POLICIES,
    VALID_LAYERS,
    VALID_MERGE_STRATEGIES,
    VALID_OBJECT_STORAGE_PROVIDERS,
    VALID_QUALITY_FAIL_ACTIONS,
    VALID_QUALITY_RULE_SEVERITIES,
    VALID_SCHEMA_POLICIES,
    VALID_SOURCE_TRIGGERS,
    VALID_SOURCE_TYPES,
    VALID_WRITE_MODES,
    WriteMode,
)
from .governance import (
    AccessContract,
    AnnotationsContract,
    OperationsContract,
    normalize_access_contract,
    normalize_annotations_contract,
    normalize_operations_contract,
)
from .hooks import IngestionHooks, normalize_hooks
from .presets import apply_preset
from .shape import ShapeConfig, normalize_shape
from ._sql import as_list, full_table_name


_QUALITY_RULE_FIELDS = {
    "required_columns",
    "not_null",
    "unique_key",
    "accepted_values",
    "min_rows",
    "max_null_ratio",
    "expressions",
    "custom",
}


def _require_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} deve ser um objeto/dict")
    return value


def _require_positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{field} deve ser inteiro positivo") from exc
    if parsed <= 0:
        raise ValueError(f"{field} deve ser inteiro positivo")
    return parsed


def _require_non_negative_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{field} deve ser inteiro não negativo") from exc
    if parsed < 0:
        raise ValueError(f"{field} deve ser inteiro não negativo")
    return parsed


def _require_ratio(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(f"{field} deve ser número entre 0 e 1") from exc
    if parsed < 0 or parsed > 1:
        raise ValueError(f"{field} deve ser número entre 0 e 1")
    return parsed


_CONNECTOR_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_FILE_CONNECTORS = {"csv", "delta", "json", "orc", "parquet", "text"}
_OBJECT_STORAGE_CONNECTORS = {"adls", "azure_blob", "blob", "gcs", "object_storage", "s3"}
_OBJECT_STORAGE_CONNECTOR_PROVIDERS = {"adls": "adls", "azure_blob": "azure_blob", "gcs": "gcs", "s3": "s3"}
_JDBC_CONNECTORS = {"jdbc", "mysql", "oracle", "postgres", "postgresql", "sqlserver"}
_SPARK_FORMAT_CONNECTORS = {"bigquery", "snowflake"}


def _validate_connector_name(value: Any, field: str = "source.connector") -> str:
    connector = str(value or "").strip()
    if not connector:
        raise ValueError(f"{field} é obrigatório quando source.type=connector")
    if not _CONNECTOR_NAME_RE.match(connector):
        raise ValueError(
            f"{field}={connector!r} é inválido. Use letras, números, '_' ou '-', "
            "começando por letra."
        )
    return connector


def _normalize_named_list(value: Any, field: str) -> List[str]:
    if isinstance(value, Mapping):
        raise ValueError(f"{field} deve ser lista ou string separada por '|', não dict")
    items = as_list(value)
    if any(not item for item in items):
        raise ValueError(f"{field} não pode conter valores vazios")
    return items


def _normalize_value_list(value: Any, field: str) -> List[Any]:
    if value is None:
        raise ValueError(f"{field} não pode ser vazio")
    if isinstance(value, str):
        values = as_list(value)
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = [value]
    if not values:
        raise ValueError(f"{field} não pode ser vazio")
    return values


def _normalize_string_mapping(value: Any, field: str) -> Dict[str, str]:
    if value is None:
        return {}
    raw = _require_mapping(value, field)
    normalized = {}
    for key, val in raw.items():
        source = str(key).strip()
        target = str(val).strip()
        if not source or not target:
            raise ValueError(f"{field} não pode conter chave ou valor vazio")
        normalized[source] = target
    return normalized


def _normalize_delta_properties(value: Any) -> Dict[str, str]:
    if value is None:
        return {}
    raw = _require_mapping(value, "delta_properties")
    normalized = {}
    for key, val in raw.items():
        prop = str(key).strip()
        prop_value = str(val).lower().strip() if isinstance(val, bool) else str(val).strip()
        if not prop or not prop_value:
            raise ValueError("delta_properties não pode conter chave ou valor vazio")
        normalized[prop] = prop_value
    return normalized


def _normalize_options(value: Any, field: str) -> Dict[str, Any]:
    if value is None:
        return {}
    return dict(_require_mapping(value, field))


def _normalize_connector_source(raw: Mapping[str, Any]) -> "ConnectorSpec":
    connector = _validate_connector_name(raw.get("connector"))
    return ConnectorSpec(
        connector=connector,
        name=(str(raw["name"]).strip() if raw.get("name") is not None else None),
        provider=(str(raw["provider"]).strip() if raw.get("provider") is not None else None),
        format=(str(raw["format"]).strip() if raw.get("format") is not None else None),
        path=(str(raw["path"]).strip() if raw.get("path") is not None else None),
        table=(str(raw["table"]).strip() if raw.get("table") is not None else None),
        query=(str(raw["query"]).strip() if raw.get("query") is not None else None),
        options=_normalize_options(raw.get("options"), "source.options"),
        read=_normalize_options(raw.get("read"), "source.read"),
        request=_normalize_options(raw.get("request"), "source.request"),
        auth=_normalize_options(raw.get("auth"), "source.auth"),
        pagination=_normalize_options(raw.get("pagination"), "source.pagination"),
        response=_normalize_options(raw.get("response"), "source.response"),
        incremental=_normalize_options(raw.get("incremental"), "source.incremental"),
        limits=_normalize_options(raw.get("limits"), "source.limits"),
    )


def _connector_value(spec: "ConnectorSpec", attr: str) -> Any:
    value = getattr(spec, attr)
    if value is not None and value != "":
        return value
    return spec.options.get(attr)


def _validate_native_connector_contract(spec: "ConnectorSpec") -> None:
    connector = spec.connector
    if connector == "autoloader":
        if not spec.path:
            raise ValueError("source.path é obrigatório para connector=autoloader")
        schema_location = spec.read.get("schema_location") or spec.options.get("cloudFiles.schemaLocation")
        if not schema_location:
            raise ValueError("source.read.schema_location é obrigatório para connector=autoloader")
        if not spec.read.get("checkpoint_location"):
            raise ValueError("source.read.checkpoint_location é obrigatório para connector=autoloader")
        return
    if connector in {"table", "delta_table", "view"}:
        if not _connector_value(spec, "table") and not spec.path:
            raise ValueError("source.table é obrigatório para connector=table/delta_table/view")
        return
    if connector == "sql":
        if not _connector_value(spec, "query"):
            raise ValueError("source.query é obrigatório para connector=sql")
        return
    if connector in _FILE_CONNECTORS:
        if not spec.path and not spec.options.get("path"):
            raise ValueError(f"source.path é obrigatório para connector={connector}")
        return
    if connector in _OBJECT_STORAGE_CONNECTORS:
        provider = str(spec.provider or "").strip()
        if provider and provider not in VALID_OBJECT_STORAGE_PROVIDERS:
            raise ValueError(
                f"source.provider={provider!r} não é suportado. "
                f"Valores válidos: {sorted(VALID_OBJECT_STORAGE_PROVIDERS)}"
            )
        expected_provider = _OBJECT_STORAGE_CONNECTOR_PROVIDERS.get(connector)
        if provider and expected_provider and provider != expected_provider:
            raise ValueError(
                f"source.provider={provider!r} conflita com connector={connector!r}; "
                f"use provider={expected_provider!r} ou remova provider"
            )
        fmt = str(spec.format or "").strip()
        if not fmt:
            raise ValueError("source.format é obrigatório para connector=object_storage/blob")
        if fmt not in VALID_FILE_CONNECTOR_FORMATS:
            raise ValueError(f"source.format={fmt!r} não é suportado. Válidos: {sorted(VALID_FILE_CONNECTOR_FORMATS)}")
        if not spec.path and not spec.options.get("path"):
            raise ValueError(f"source.path é obrigatório para connector={connector}")
        return
    if connector in _JDBC_CONNECTORS:
        if "url" not in spec.options:
            raise ValueError(f"source.options.url é obrigatório para connector={connector}")
        if "dbtable" not in spec.options and "query" not in spec.options:
            raise ValueError(f"connector={connector} requer source.options.dbtable ou source.options.query")
        partition_fields = {"partition_column", "lower_bound", "upper_bound", "num_partitions"}
        provided = {field for field in partition_fields if spec.read.get(field) is not None}
        if provided and provided != partition_fields:
            raise ValueError(
                "JDBC partitioning requer partition_column, lower_bound, upper_bound e num_partitions juntos"
            )
        return
    if connector in _SPARK_FORMAT_CONNECTORS:
        if "query" not in spec.options and "dbtable" not in spec.options and "table" not in spec.options and not spec.table:
            raise ValueError(
                f"connector={connector} requer source.table, source.options.table, "
                "source.options.dbtable ou source.options.query"
            )
        return
    if connector == "rest_api":
        request = spec.request or {}
        if not request.get("url") and not spec.path:
            raise ValueError("source.request.url é obrigatório para connector=rest_api")
        method = str(request.get("method") or "GET").upper()
        if method not in {"GET", "POST"}:
            raise ValueError("connector=rest_api suporta apenas GET e POST")
        auth_type = str((spec.auth or {}).get("type") or "none")
        if auth_type not in {"none", "bearer_token", "api_key", "basic", "oauth_client_credentials"}:
            raise ValueError(f"auth.type={auth_type!r} não suportado para connector=rest_api")
        page_type = str((spec.pagination or {}).get("type") or "none")
        if page_type not in {"none", "page", "offset", "cursor", "link_header"}:
            raise ValueError(f"pagination.type={page_type!r} não suportado")
        if page_type == "cursor" and not spec.pagination.get("next_cursor_path"):
            raise ValueError("pagination.next_cursor_path é obrigatório quando pagination.type=cursor")
        if spec.incremental.get("watermark_body_field") and "json" not in request:
            raise ValueError("source.incremental.watermark_body_field exige source.request.json")


def _normalize_source(value: Any) -> Source:
    if isinstance(value, (SourceSpec, ConnectorSpec)):
        return value
    if isinstance(value, Mapping):
        raw = _require_mapping(value, "source")
        source_type = _validate_enum(raw.get("type"), VALID_SOURCE_TYPES, "source.type")
        if source_type == "connector":
            return _normalize_connector_source(raw)
        path = str(raw.get("path") or "").strip()
        if not path:
            raise ValueError("source.path é obrigatório quando source é declarativo")
        trigger = _validate_enum(
            raw.get("trigger", "available_now"),
            VALID_SOURCE_TRIGGERS,
            "source.trigger",
            default="available_now",
        )
        schema_location = str(raw.get("schema_location") or "").strip()
        checkpoint_location = str(raw.get("checkpoint_location") or "").strip()
        if not schema_location:
            raise ValueError("source.schema_location é obrigatório para source.type=autoloader")
        if not checkpoint_location:
            raise ValueError("source.checkpoint_location é obrigatório para source.trigger=available_now")
        max_files = raw.get("max_files_per_trigger")
        return SourceSpec(
            type=source_type,  # type: ignore[arg-type]
            path=path,
            format=str(raw.get("format") or "parquet").strip() or "parquet",
            schema_location=schema_location,
            checkpoint_location=checkpoint_location,
            trigger=trigger,  # type: ignore[arg-type]
            options=_normalize_options(raw.get("options"), "source.options"),
            schema_hints=raw.get("schema_hints"),
            include_existing_files=bool(raw.get("include_existing_files", True)),
            max_files_per_trigger=(
                None
                if max_files is None
                else _require_positive_int(max_files, "source.max_files_per_trigger")
            ),
        )
    return value


def _normalize_quality_expression(item: Any) -> QualityExpression:
    if isinstance(item, QualityExpression):
        return item
    raw = _require_mapping(item, "quality_rules.expressions[]")
    name = str(raw.get("name") or "").strip()
    expression = str(raw.get("expression") or "").strip()
    return QualityExpression(
        name=name,
        expression=expression,
        severity=_validate_enum(
            raw.get("severity", "quarantine"),
            VALID_QUALITY_RULE_SEVERITIES,
            "quality_rules.expressions.severity",
            default="quarantine",
        ),
        message=raw.get("message"),
    )


@dataclass(frozen=True)
class QualityExpression:
    """Regra de qualidade baseada em expressão SQL booleana."""

    name: str
    expression: str
    severity: QualityRuleSeverity = "quarantine"
    message: Optional[str] = None


@dataclass(frozen=True)
class QualityRules:
    """Regras de qualidade avaliadas antes da escrita.

    Frozen para passagem segura entre threads. Construtores aceitam dict via
    ``normalize_quality_rules``.

    Attributes:
        required_columns: Colunas obrigatórias no schema.
        not_null: Colunas que não podem ter valores NULL.
        unique_key: Conjunto de colunas que forma chave única.
        accepted_values: Mapa coluna -> lista de valores aceitos. Limitado por
            ``CONFIG.max_inline_accepted_values``.
        min_rows: Mínimo de linhas no DataFrame após watermark/dedup.
        max_null_ratio: Mapa coluna -> razão máxima de NULLs (0.0 a 1.0).
    """

    required_columns: List[str] = field(default_factory=list)
    not_null: List[str] = field(default_factory=list)
    unique_key: List[str] = field(default_factory=list)
    accepted_values: Dict[str, List[Any]] = field(default_factory=dict)
    min_rows: Optional[int] = None
    max_null_ratio: Dict[str, float] = field(default_factory=dict)
    expressions: List[QualityExpression] = field(default_factory=list)
    custom: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceSpec:
    """Source declarativo. Atualmente suporta Autoloader ``available_now``."""

    type: Literal["autoloader"]
    path: str
    format: str = "parquet"
    schema_location: str = ""
    checkpoint_location: str = ""
    trigger: Literal["available_now"] = "available_now"
    options: Dict[str, Any] = field(default_factory=dict)
    schema_hints: Optional[str] = None
    include_existing_files: bool = True
    max_files_per_trigger: Optional[int] = None


@dataclass(frozen=True)
class ConnectorSpec:
    """Source declarativo genérico resolvido por registry de conectores."""

    type: Literal["connector"] = "connector"
    connector: str = ""
    name: Optional[str] = None
    provider: Optional[str] = None
    format: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    read: Dict[str, Any] = field(default_factory=dict)
    request: Dict[str, Any] = field(default_factory=dict)
    auth: Dict[str, Any] = field(default_factory=dict)
    pagination: Dict[str, Any] = field(default_factory=dict)
    response: Dict[str, Any] = field(default_factory=dict)
    incremental: Dict[str, Any] = field(default_factory=dict)
    limits: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IngestionPlan:
    """Contrato declarativo de ingestão de uma tabela.

    Frozen — todas as decisões da execução ficam fixadas no momento da
    construção. Uma instância representa uma execução do orquestrador
    ``ingest_plan``.

    Os campos estão agrupados por finalidade. Veja ``docs/arquitetura.md``
    para descrição detalhada de cada um.
    """

    source: Source
    target_table: str
    catalog: str = "main"
    layer: Layer = "bronze"
    target_schema: Optional[str] = None
    mode: WriteMode = "scd0_append"
    source_system: str = "default"
    ctrl_schema: str = "ops"
    notebook_name: str = "unknown"
    description: Optional[str] = None
    owner: Optional[str] = None
    domain: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    sla: Optional[str] = None
    runtime_parameters: Dict[str, Any] = field(default_factory=dict)

    select_columns: List[str] = field(default_factory=list)
    column_mapping: Dict[str, str] = field(default_factory=dict)
    shape: Optional[ShapeConfig] = None
    filter_expression: Optional[str] = None
    watermark_columns: List[str] = field(default_factory=list)
    merge_keys: List[str] = field(default_factory=list)
    hash_keys: List[str] = field(default_factory=list)
    hash_exclude_columns: List[str] = field(default_factory=list)
    custom_keys: Dict[str, List[str]] = field(default_factory=dict)
    dedup_order_expr: Optional[str] = None

    partition_column: Optional[str] = None
    partition_value: Optional[str] = None
    merge_strategy: MergeStrategy = "delta"
    merge_partition_column: Optional[str] = None
    replace_partitions_source_complete: bool = False
    cluster_columns: List[str] = field(default_factory=list)
    zorder_columns: List[str] = field(default_factory=list)
    optimize_after_write: bool = False
    delta_properties: Dict[str, str] = field(default_factory=dict)

    schema_policy: SchemaPolicy = "permissive"
    allow_type_widening: bool = False
    quality_rules: Optional[QualityRules] = None
    on_quality_fail: QualityFailAction = "fail"

    scd2_change_columns: List[str] = field(default_factory=list)
    scd2_effective_from_column: Optional[str] = None

    fix_encoding: bool = False
    encoding: str = "Windows-1252"
    encoding_columns: List[str] = field(default_factory=list)

    dry_run: bool = False
    explain_mode: bool = False
    explain_format: str = "formatted"
    openlineage_enabled: bool = False
    openlineage_namespace: Optional[str] = None
    openlineage_producer: str = "contractforge"
    use_cache: bool = True
    lock_enabled: bool = False
    idempotency_key: Optional[str] = None
    idempotency_policy: IdempotencyPolicy = "always_run"
    retry_attempts: Optional[int] = None
    retry_backoff_seconds: Optional[int] = None
    hooks: Optional[IngestionHooks] = None
    annotations: Optional[AnnotationsContract] = None
    operations: Optional[OperationsContract] = None
    access: Optional[AccessContract] = None
    applied_presets: List[str] = field(default_factory=list)
    parent_run_id: Optional[str] = None
    run_group_id: Optional[str] = None
    master_job_id: Optional[str] = None
    master_run_id: Optional[str] = None


def validate_write_mode(mode: Optional[str]) -> WriteMode:
    """Valida e normaliza o modo de escrita.

    ``None`` ou string vazia caem para ``scd0_append``.

    Raises:
        ValueError: se ``mode`` não estiver em ``VALID_WRITE_MODES``.
    """
    raw = (mode or "scd0_append").strip()
    if raw not in VALID_WRITE_MODES:
        raise ValueError(
            f"Modo de escrita não suportado: {raw}. Modos válidos: {sorted(VALID_WRITE_MODES)}"
        )
    return raw  # type: ignore[return-value]


def _validate_enum(value: Any, valid: set, param: str, default: Optional[str] = None) -> str:
    """Valida que ``value`` pertence ao conjunto ``valid`` ou cai para ``default``.

    Raises:
        ValueError: se ``value`` é truthy mas não está em ``valid``.
    """
    if value is None or value == "":
        if default is None:
            raise ValueError(f"{param} é obrigatório. Valores válidos: {sorted(valid)}")
        return default
    raw = str(value).strip()
    if raw not in valid:
        raise ValueError(f"{param}={raw!r} não é suportado. Valores válidos: {sorted(valid)}")
    return raw


def normalize_quality_rules(
    value: Optional[Union[QualityRules, Dict[str, Any]]],
) -> Optional[QualityRules]:
    """Aceita ``None``, ``QualityRules`` ou ``dict`` e devolve ``QualityRules``.

    Conveniência para que YAMLs possam declarar ``quality_rules`` como dict
    diretamente, sem importar a dataclass.
    """
    if value is None:
        return None
    if isinstance(value, QualityRules):
        return value
    raw = dict(_require_mapping(value, "quality_rules"))
    unexpected = set(raw) - _QUALITY_RULE_FIELDS
    if unexpected:
        raise ValueError(f"quality_rules possui campos não reconhecidos: {sorted(unexpected)}")

    normalized: Dict[str, Any] = {
        "required_columns": _normalize_named_list(raw.get("required_columns"), "quality_rules.required_columns"),
        "not_null": _normalize_named_list(raw.get("not_null"), "quality_rules.not_null"),
        "unique_key": _normalize_named_list(raw.get("unique_key"), "quality_rules.unique_key"),
        "accepted_values": {},
        "min_rows": None,
        "max_null_ratio": {},
        "expressions": [],
        "custom": {},
    }

    accepted_values = {} if raw.get("accepted_values") is None else raw["accepted_values"]
    for column, values in _require_mapping(accepted_values, "quality_rules.accepted_values").items():
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("quality_rules.accepted_values possui coluna vazia")
        normalized["accepted_values"][column_name] = _normalize_value_list(
            values,
            f"quality_rules.accepted_values.{column_name}",
        )

    if raw.get("min_rows") is not None:
        normalized["min_rows"] = _require_positive_int(raw["min_rows"], "quality_rules.min_rows")

    max_null_ratio = {} if raw.get("max_null_ratio") is None else raw["max_null_ratio"]
    for column, ratio in _require_mapping(max_null_ratio, "quality_rules.max_null_ratio").items():
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("quality_rules.max_null_ratio possui coluna vazia")
        normalized["max_null_ratio"][column_name] = _require_ratio(
            ratio,
            f"quality_rules.max_null_ratio.{column_name}",
        )

    raw_expressions = [] if raw.get("expressions") is None else raw["expressions"]
    if isinstance(raw_expressions, Mapping) or isinstance(raw_expressions, str):
        raise ValueError("quality_rules.expressions deve ser uma lista")
    expression_names = set()
    normalized["expressions"] = [_normalize_quality_expression(item) for item in raw_expressions]
    for rule in normalized["expressions"]:
        if not rule.name or not rule.expression:
            raise ValueError("quality_rules.expressions requer name e expression")
        if rule.name in expression_names:
            raise ValueError(f"quality_rules.expressions possui name duplicado: {rule.name}")
        expression_names.add(rule.name)

    raw_custom = {} if raw.get("custom") is None else raw["custom"]
    for rule_name, rule_config in _require_mapping(raw_custom, "quality_rules.custom").items():
        name = str(rule_name).strip()
        if not name:
            raise ValueError("quality_rules.custom possui nome vazio")
        config = dict(_require_mapping(rule_config, f"quality_rules.custom.{name}"))
        rule_type = str(config.get("type") or "").strip()
        if not rule_type:
            raise ValueError(f"quality_rules.custom.{name}.type é obrigatório")
        if "severity" in config:
            config["severity"] = _validate_enum(
                config["severity"],
                VALID_QUALITY_RULE_SEVERITIES,
                f"quality_rules.custom.{name}.severity",
                default="abort",
            )
        normalized["custom"][name] = config
    return QualityRules(**normalized)


_KNOWN_PARAMS = {
    "source", "target", "target_table", "catalog", "layer", "target_schema", "mode", "source_system", "ctrl_schema",
    "notebook_name", "description", "owner", "domain", "tags", "sla", "runtime_parameters",
    "select_columns", "column_mapping", "shape", "filter_expression", "watermark_columns",
    "merge_keys", "hash_keys", "hash_exclude_columns", "custom_keys", "dedup_order_expr",
    "partition_column", "partition_value", "merge_strategy", "merge_partition_column",
    "replace_partitions_source_complete", "cluster_columns", "zorder_columns", "optimize_after_write",
    "delta_properties", "schema_policy", "allow_type_widening", "quality_rules", "on_quality_fail",
    "scd2_change_columns", "scd2_effective_from_column",
    "fix_encoding", "encoding", "encoding_columns", "dry_run", "explain_mode",
    "explain_format", "openlineage_enabled", "openlineage_namespace",
    "openlineage_producer", "use_cache", "lock_enabled", "idempotency_key",
    "idempotency_policy", "retry_attempts", "retry_backoff_seconds", "hooks",
    "annotations", "operations", "access", "preset", "presets", "applied_presets",
    "parent_run_id", "run_group_id", "master_job_id", "master_run_id",
}


def target_schema_name(plan: IngestionPlan) -> str:
    """Schema físico do target; por padrão usa a camada lógica."""
    return str(plan.target_schema or plan.layer).strip()


def target_full_table_name(plan: IngestionPlan) -> str:
    """Nome fully-qualified do target físico do plano."""
    return full_table_name(plan.catalog, target_schema_name(plan), plan.target_table)


def _normalize_target_block(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza ``target: {catalog, schema, table}`` para campos do plano."""
    raw = kwargs.pop("target", None)
    if raw is None:
        return kwargs
    target = _require_mapping(raw, "target")
    unexpected = set(target) - {"catalog", "schema", "table"}
    if unexpected:
        raise ValueError(f"target possui campos não reconhecidos: {sorted(unexpected)}")
    aliases = {
        "catalog": "catalog",
        "schema": "target_schema",
        "table": "target_table",
    }
    for source_field, plan_field in aliases.items():
        raw_value = target.get(source_field)
        if raw_value is None or raw_value == "":
            continue
        value = str(raw_value).strip()
        if not value:
            raise ValueError(f"target.{source_field} não pode ser vazio")
        existing = kwargs.get(plan_field)
        if existing is not None and str(existing).strip() and str(existing).strip() != value:
            raise ValueError(
                f"target.{source_field}={value!r} conflita com {plan_field}={str(existing).strip()!r}"
            )
        kwargs[plan_field] = value
    return kwargs


def validate_plan_shape(plan: IngestionPlan) -> None:
    """Valida campos declarativos que independem de Spark/DataFrame.

    Esta validação não substitui as regras de modo aplicadas no orquestrador,
    mas pega contratos malformados cedo, especialmente YAMLs.
    """
    required_text_fields = {
        "target_table": plan.target_table,
        "catalog": plan.catalog,
        "layer": plan.layer,
        "mode": plan.mode,
        "source_system": plan.source_system,
        "ctrl_schema": plan.ctrl_schema,
        "notebook_name": plan.notebook_name,
    }
    for field_name, value in required_text_fields.items():
        if not str(value or "").strip():
            raise ValueError(f"{field_name} é obrigatório e não pode ser vazio")
    if plan.target_schema is not None and not str(plan.target_schema).strip():
        raise ValueError("target_schema não pode ser vazio")
    if not isinstance(plan.runtime_parameters, dict):
        raise ValueError("runtime_parameters deve ser dict")
    if any(not str(tag).strip() for tag in plan.tags):
        raise ValueError("tags não pode conter valores vazios")
    if plan.idempotency_policy != "always_run" and not plan.idempotency_key:
        raise ValueError("idempotency_policy diferente de always_run requer idempotency_key")
    if plan.retry_attempts is not None and plan.retry_attempts <= 0:
        raise ValueError("retry_attempts deve ser inteiro positivo")
    if plan.retry_backoff_seconds is not None and plan.retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds deve ser inteiro não negativo")
    if plan.allow_type_widening and plan.schema_policy == "strict":
        raise ValueError("allow_type_widening=True é incompatível com schema_policy=strict")
    if len(set(plan.column_mapping.values())) != len(plan.column_mapping):
        raise ValueError("column_mapping não pode mapear múltiplas colunas para o mesmo destino")
    reserved_mapping_targets = sorted(set(plan.column_mapping.values()) & CONTROL_COLUMNS)
    if reserved_mapping_targets:
        raise ValueError(
            f"column_mapping não pode produzir colunas técnicas reservadas: {reserved_mapping_targets}"
        )
    if isinstance(plan.source, SourceSpec):
        if plan.source.type not in VALID_SOURCE_TYPES:
            raise ValueError(f"source.type={plan.source.type!r} não é suportado")
        if not plan.source.path:
            raise ValueError("source.path é obrigatório quando source é declarativo")
        if not plan.source.schema_location:
            raise ValueError("source.schema_location é obrigatório para source.type=autoloader")
        if plan.source.trigger not in VALID_SOURCE_TRIGGERS:
            raise ValueError(f"source.trigger={plan.source.trigger!r} não é suportado")
        if not plan.source.checkpoint_location:
            raise ValueError("source.checkpoint_location é obrigatório para source.trigger=available_now")
        if plan.mode == "snapshot_soft_delete":
            raise ValueError("snapshot_soft_delete é incompatível com sources incrementais declarativos")
    if isinstance(plan.source, ConnectorSpec):
        if plan.source.type != "connector":
            raise ValueError("ConnectorSpec.type deve ser 'connector'")
        _validate_connector_name(plan.source.connector)
        _validate_native_connector_contract(plan.source)
    if plan.quality_rules:
        if plan.quality_rules.min_rows is not None and plan.quality_rules.min_rows <= 0:
            raise ValueError("quality_rules.min_rows deve ser inteiro positivo")
        for column, ratio in plan.quality_rules.max_null_ratio.items():
            if ratio < 0 or ratio > 1:
                raise ValueError(f"quality_rules.max_null_ratio.{column} deve estar entre 0 e 1")


def build_plan_from_kwargs(**kwargs: Any) -> IngestionPlan:
    """Constrói um ``IngestionPlan`` a partir de kwargs estilo notebook.

    Diferente de ``IngestionPlan(**kwargs)`` direto, esta função:

    - Rejeita parâmetros desconhecidos (pega typos como ``merg_keys``).
    - Aceita listas em string com separador ``|`` (ex.: ``"a|b|c"``).
    - Aceita ``quality_rules`` como ``dict`` ou ``QualityRules``.
    - Aceita ``custom_keys`` com listas em string.
    - Valida ``mode`` via ``validate_write_mode``.

    É a porta de entrada recomendada para uso a partir de YAML/notebook.

    Raises:
        ValueError: se houver kwargs desconhecidos ou ``mode`` inválido.
    """
    kwargs = _normalize_target_block(apply_preset(dict(kwargs)))
    quality = normalize_quality_rules(kwargs.pop("quality_rules", None))
    custom = kwargs.pop("custom_keys", None) or {}
    normalized_custom = {k: as_list(v) for k, v in custom.items()}

    unexpected = set(kwargs) - _KNOWN_PARAMS
    if unexpected:
        raise ValueError(f"Parâmetros não reconhecidos em ingest(): {sorted(unexpected)}")
    if "source" not in kwargs:
        raise ValueError("source é obrigatório")
    if "target_table" not in kwargs:
        raise ValueError("target_table é obrigatório ou use target.table")

    layer = _validate_enum(kwargs.get("layer", "bronze"), VALID_LAYERS, "layer", default="bronze")
    merge_strategy = _validate_enum(
        kwargs.get("merge_strategy", "delta"), VALID_MERGE_STRATEGIES, "merge_strategy", default="delta"
    )
    schema_policy = _validate_enum(
        kwargs.get("schema_policy", "permissive"),
        VALID_SCHEMA_POLICIES,
        "schema_policy",
        default="permissive",
    )
    on_quality_fail = _validate_enum(
        kwargs.get("on_quality_fail", "fail"),
        VALID_QUALITY_FAIL_ACTIONS,
        "on_quality_fail",
        default="fail",
    )
    explain_format = _validate_enum(
        kwargs.get("explain_format", "formatted"),
        VALID_EXPLAIN_FORMATS,
        "explain_format",
        default="formatted",
    )
    idempotency_policy = _validate_enum(
        kwargs.get("idempotency_policy", "always_run"),
        VALID_IDEMPOTENCY_POLICIES,
        "idempotency_policy",
        default="always_run",
    )
    plan = IngestionPlan(
        source=_normalize_source(kwargs["source"]),
        target_table=kwargs["target_table"],
        catalog=kwargs.get("catalog", CONFIG.default_catalog),
        layer=layer,  # type: ignore[arg-type]
        target_schema=kwargs.get("target_schema"),
        mode=validate_write_mode(kwargs.get("mode", "scd0_append")),
        source_system=kwargs.get("source_system", CONFIG.default_source_system),
        ctrl_schema=kwargs.get("ctrl_schema", CONFIG.ctrl_schema),
        notebook_name=kwargs.get("notebook_name", "unknown"),
        description=kwargs.get("description"),
        owner=kwargs.get("owner"),
        domain=kwargs.get("domain"),
        tags=as_list(kwargs.get("tags")),
        sla=kwargs.get("sla"),
        runtime_parameters=dict(kwargs.get("runtime_parameters") or {}),
        select_columns=as_list(kwargs.get("select_columns")),
        column_mapping=_normalize_string_mapping(kwargs.get("column_mapping"), "column_mapping"),
        shape=normalize_shape(kwargs.get("shape")),
        filter_expression=kwargs.get("filter_expression"),
        watermark_columns=as_list(kwargs.get("watermark_columns")),
        merge_keys=as_list(kwargs.get("merge_keys")),
        hash_keys=as_list(kwargs.get("hash_keys")),
        hash_exclude_columns=as_list(kwargs.get("hash_exclude_columns")),
        custom_keys=normalized_custom,
        dedup_order_expr=kwargs.get("dedup_order_expr"),
        partition_column=kwargs.get("partition_column"),
        partition_value=kwargs.get("partition_value"),
        merge_strategy=merge_strategy,  # type: ignore[arg-type]
        merge_partition_column=kwargs.get("merge_partition_column"),
        replace_partitions_source_complete=bool(kwargs.get("replace_partitions_source_complete", False)),
        cluster_columns=as_list(kwargs.get("cluster_columns")),
        zorder_columns=as_list(kwargs.get("zorder_columns")),
        optimize_after_write=bool(kwargs.get("optimize_after_write", False)),
        delta_properties=_normalize_delta_properties(kwargs.get("delta_properties")),
        schema_policy=schema_policy,  # type: ignore[arg-type]
        allow_type_widening=bool(kwargs.get("allow_type_widening", False)),
        quality_rules=quality,
        on_quality_fail=on_quality_fail,  # type: ignore[arg-type]
        scd2_change_columns=as_list(kwargs.get("scd2_change_columns")),
        scd2_effective_from_column=kwargs.get("scd2_effective_from_column"),
        fix_encoding=bool(kwargs.get("fix_encoding", False)),
        encoding=kwargs.get("encoding", "Windows-1252"),
        encoding_columns=as_list(kwargs.get("encoding_columns")),
        dry_run=bool(kwargs.get("dry_run", False)),
        explain_mode=bool(kwargs.get("explain_mode", False)),
        explain_format=explain_format,
        openlineage_enabled=bool(kwargs.get("openlineage_enabled", False)),
        openlineage_namespace=kwargs.get("openlineage_namespace"),
        openlineage_producer=kwargs.get("openlineage_producer", "contractforge"),
        use_cache=bool(kwargs.get("use_cache", True)),
        lock_enabled=bool(kwargs.get("lock_enabled", False)),
        idempotency_key=kwargs.get("idempotency_key"),
        idempotency_policy=idempotency_policy,  # type: ignore[arg-type]
        retry_attempts=(
            None
            if kwargs.get("retry_attempts") is None
            else _require_positive_int(kwargs.get("retry_attempts"), "retry_attempts")
        ),
        retry_backoff_seconds=(
            None
            if kwargs.get("retry_backoff_seconds") is None
            else _require_non_negative_int(
                kwargs.get("retry_backoff_seconds"),
                "retry_backoff_seconds",
            )
        ),
        hooks=normalize_hooks(kwargs.get("hooks")),
        annotations=normalize_annotations_contract(kwargs.get("annotations")),
        operations=normalize_operations_contract(kwargs.get("operations")),
        access=normalize_access_contract(kwargs.get("access")),
        applied_presets=as_list(kwargs.get("applied_presets")),
        parent_run_id=kwargs.get("parent_run_id"),
        run_group_id=kwargs.get("run_group_id"),
        master_job_id=kwargs.get("master_job_id"),
        master_run_id=kwargs.get("master_run_id"),
    )
    validate_plan_shape(plan)
    return plan
