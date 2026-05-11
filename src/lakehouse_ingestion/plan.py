"""Contratos declarativos: IngestionPlan, QualityRules e construtor a partir de kwargs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from .config import (
    CONFIG,
    IdempotencyPolicy,
    Layer,
    MergeStrategy,
    QualityFailAction,
    QualityRuleSeverity,
    SchemaPolicy,
    Source,
    VALID_EXPLAIN_FORMATS,
    VALID_IDEMPOTENCY_POLICIES,
    VALID_LAYERS,
    VALID_MERGE_STRATEGIES,
    VALID_QUALITY_FAIL_ACTIONS,
    VALID_QUALITY_RULE_SEVERITIES,
    VALID_SCHEMA_POLICIES,
    VALID_WRITE_MODES,
    WriteMode,
)
from ._sql import as_list


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
    openlineage_producer: str = "lakehouse-ingestion-framework"
    use_cache: bool = True
    lock_enabled: bool = False
    idempotency_key: Optional[str] = None
    idempotency_policy: IdempotencyPolicy = "always_run"
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
    normalized = dict(value)
    expressions = normalized.get("expressions") or []
    normalized["expressions"] = [
        item
        if isinstance(item, QualityExpression)
        else QualityExpression(
            name=str(item["name"]),
            expression=str(item["expression"]),
            severity=_validate_enum(
                item.get("severity", "quarantine"),
                VALID_QUALITY_RULE_SEVERITIES,
                "quality_rules.expressions.severity",
                default="quarantine",
            ),
            message=item.get("message"),
        )
        for item in expressions
    ]
    return QualityRules(**normalized)


_KNOWN_PARAMS = {
    "source", "target_table", "catalog", "layer", "mode", "source_system", "ctrl_schema",
    "notebook_name", "description", "owner", "domain", "tags", "sla", "runtime_parameters",
    "select_columns", "filter_expression", "watermark_columns",
    "merge_keys", "hash_keys", "hash_exclude_columns", "custom_keys", "dedup_order_expr",
    "partition_column", "partition_value", "merge_strategy", "merge_partition_column",
    "replace_partitions_source_complete", "cluster_columns", "zorder_columns", "optimize_after_write",
    "schema_policy", "allow_type_widening", "quality_rules", "on_quality_fail",
    "scd2_change_columns", "scd2_effective_from_column",
    "fix_encoding", "encoding", "encoding_columns", "dry_run", "explain_mode",
    "explain_format", "openlineage_enabled", "openlineage_namespace",
    "openlineage_producer", "use_cache", "lock_enabled", "idempotency_key",
    "idempotency_policy", "parent_run_id", "run_group_id",
    "master_job_id", "master_run_id",
}


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
    quality = normalize_quality_rules(kwargs.pop("quality_rules", None))
    custom = kwargs.pop("custom_keys", None) or {}
    normalized_custom = {k: as_list(v) for k, v in custom.items()}

    unexpected = set(kwargs) - _KNOWN_PARAMS
    if unexpected:
        raise ValueError(f"Parâmetros não reconhecidos em ingest(): {sorted(unexpected)}")

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
    return IngestionPlan(
        source=kwargs["source"],
        target_table=kwargs["target_table"],
        catalog=kwargs.get("catalog", CONFIG.default_catalog),
        layer=layer,  # type: ignore[arg-type]
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
        openlineage_producer=kwargs.get("openlineage_producer", "lakehouse-ingestion-framework"),
        use_cache=bool(kwargs.get("use_cache", True)),
        lock_enabled=bool(kwargs.get("lock_enabled", False)),
        idempotency_key=kwargs.get("idempotency_key"),
        idempotency_policy=idempotency_policy,  # type: ignore[arg-type]
        parent_run_id=kwargs.get("parent_run_id"),
        run_group_id=kwargs.get("run_group_id"),
        master_job_id=kwargs.get("master_job_id"),
        master_run_id=kwargs.get("master_run_id"),
    )
