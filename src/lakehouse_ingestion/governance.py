"""Contratos de governanca: annotations, operations e access.

Este modulo concentra normalizacao, validacao e aplicacao de metadados de
catalogo. A ingestao continua responsavel por dados; governanca fica em uma
camada declarativa separada, mas aplicavel junto do ciclo de execucao.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from ._spark import spark
from ._sql import q, qt, sql_lit, to_json, utc_now_str
from .config import (
    AccessDriftPolicy,
    AccessMode,
    GovernanceFailurePolicy,
    VALID_ACCESS_DRIFT_POLICIES,
    VALID_ACCESS_MODES,
    VALID_CRITICALITY_LEVELS,
    VALID_GOVERNANCE_FAILURE_POLICIES,
    VALID_PII_TYPES,
    VALID_SENSITIVITY_LEVELS,
)

logger = logging.getLogger("lakehouse_ingestion")


@dataclass(frozen=True)
class PiiAnnotation:
    """Classificacao PII de uma coluna."""

    enabled: bool = False
    type: str = "unknown"
    sensitivity: str = "internal"


@dataclass(frozen=True)
class DeprecatedAnnotation:
    """Ciclo de vida de coluna/tabela marcada como deprecada."""

    since: Optional[str] = None
    replacement: Optional[str] = None
    removal_date: Optional[str] = None


@dataclass(frozen=True)
class TableAnnotations:
    """Anotacoes aplicaveis a tabela."""

    description: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    deprecated: Optional[DeprecatedAnnotation] = None


@dataclass(frozen=True)
class ColumnAnnotations:
    """Anotacoes aplicaveis a coluna."""

    description: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    pii: Optional[PiiAnnotation] = None
    deprecated: Optional[DeprecatedAnnotation] = None


@dataclass(frozen=True)
class AnnotationsContract:
    """Contrato de annotations para Unity Catalog/metastore."""

    policy: GovernanceFailurePolicy = "warn"
    table: TableAnnotations = field(default_factory=TableAnnotations)
    columns: Dict[str, ColumnAnnotations] = field(default_factory=dict)


@dataclass(frozen=True)
class OperationsContract:
    """Contrato operacional usado por dashboards/alertas externos."""

    criticality: Optional[str] = None
    expected_frequency: Optional[str] = None
    freshness_sla_minutes: Optional[int] = None
    alert_on_failure: bool = False
    alert_on_quality_fail: bool = False
    runbook_url: Optional[str] = None
    owners: List[str] = field(default_factory=list)
    groups: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AccessGrant:
    """Grant declarativo em tabela."""

    principal: str
    privileges: List[str]


@dataclass(frozen=True)
class RowFilter:
    """Filtro de linha Unity Catalog."""

    name: str
    function: str
    columns: List[str] = field(default_factory=list)
    principals: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ColumnMask:
    """Mascara de coluna Unity Catalog."""

    column: str
    function: str
    using_columns: List[str] = field(default_factory=list)
    principals: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class AccessContract:
    """Contrato de acesso e seguranca."""

    mode: AccessMode = "apply"
    on_drift: AccessDriftPolicy = "warn"
    revoke_unmanaged: bool = False
    grants: List[AccessGrant] = field(default_factory=list)
    row_filters: List[RowFilter] = field(default_factory=list)
    column_masks: List[ColumnMask] = field(default_factory=list)


def _require_mapping(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} deve ser um objeto/dict")
    return value


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split("|") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _str_map(value: Any, field: str) -> Dict[str, str]:
    if value is None:
        return {}
    raw = _require_mapping(value, field)
    result = {}
    for key, val in raw.items():
        name = str(key).strip()
        rendered = str(val).lower().strip() if isinstance(val, bool) else str(val).strip()
        if not name or not rendered:
            raise ValueError(f"{field} nao pode conter chave ou valor vazio")
        result[name] = rendered
    return result


def _enum(value: Any, valid: set[str], field: str, default: Optional[str] = None) -> str:
    if value is None or value == "":
        if default is None:
            raise ValueError(f"{field} e obrigatorio. Valores validos: {sorted(valid)}")
        return default
    raw = str(value).strip()
    if raw not in valid:
        raise ValueError(f"{field}={raw!r} nao e suportado. Valores validos: {sorted(valid)}")
    return raw


def _positive_int(value: Any, field: str) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{field} deve ser inteiro positivo") from exc
    if parsed <= 0:
        raise ValueError(f"{field} deve ser inteiro positivo")
    return parsed


def _normalize_deprecated(value: Any, field: str) -> Optional[DeprecatedAnnotation]:
    if value in (None, False):
        return None
    raw = _require_mapping(value, field)
    return DeprecatedAnnotation(
        since=raw.get("since"),
        replacement=raw.get("replacement"),
        removal_date=raw.get("removal_date"),
    )


def _normalize_pii(value: Any, field: str) -> Optional[PiiAnnotation]:
    if value in (None, False):
        return None
    raw = _require_mapping(value, field)
    enabled = bool(raw.get("enabled", True))
    pii_type = _enum(raw.get("type", "unknown"), VALID_PII_TYPES, f"{field}.type", "unknown")
    sensitivity = _enum(
        raw.get("sensitivity", "internal"),
        VALID_SENSITIVITY_LEVELS,
        f"{field}.sensitivity",
        "internal",
    )
    return PiiAnnotation(enabled=enabled, type=pii_type, sensitivity=sensitivity)


def normalize_annotations_contract(value: Any) -> Optional[AnnotationsContract]:
    """Normaliza dict/YAML em ``AnnotationsContract``."""
    if value is None or isinstance(value, AnnotationsContract):
        return value
    raw = _require_mapping(value, "annotations")
    policy = _enum(
        raw.get("policy", "warn"),
        VALID_GOVERNANCE_FAILURE_POLICIES,
        "annotations.policy",
        "warn",
    )
    table_raw = _require_mapping(raw.get("table", {}), "annotations.table")
    table = TableAnnotations(
        description=table_raw.get("description"),
        aliases=_as_list(table_raw.get("aliases")),
        tags=_str_map(table_raw.get("tags"), "annotations.table.tags"),
        deprecated=_normalize_deprecated(table_raw.get("deprecated"), "annotations.table.deprecated"),
    )
    columns = {}
    for column, config in _require_mapping(raw.get("columns", {}), "annotations.columns").items():
        column_name = str(column).strip()
        if not column_name:
            raise ValueError("annotations.columns nao pode conter coluna vazia")
        column_raw = _require_mapping(config, f"annotations.columns.{column_name}")
        columns[column_name] = ColumnAnnotations(
            description=column_raw.get("description"),
            aliases=_as_list(column_raw.get("aliases")),
            tags=_str_map(column_raw.get("tags"), f"annotations.columns.{column_name}.tags"),
            pii=_normalize_pii(column_raw.get("pii"), f"annotations.columns.{column_name}.pii"),
            deprecated=_normalize_deprecated(
                column_raw.get("deprecated"),
                f"annotations.columns.{column_name}.deprecated",
            ),
        )
    return AnnotationsContract(policy=policy, table=table, columns=columns)


def normalize_operations_contract(value: Any) -> Optional[OperationsContract]:
    """Normaliza dict/YAML em ``OperationsContract``."""
    if value is None or isinstance(value, OperationsContract):
        return value
    raw = _require_mapping(value, "operations")
    criticality = raw.get("criticality")
    if criticality is not None:
        criticality = _enum(
            criticality,
            VALID_CRITICALITY_LEVELS,
            "operations.criticality",
        )
    return OperationsContract(
        criticality=criticality,
        expected_frequency=raw.get("expected_frequency"),
        freshness_sla_minutes=_positive_int(
            raw.get("freshness_sla_minutes"),
            "operations.freshness_sla_minutes",
        ),
        alert_on_failure=bool(raw.get("alert_on_failure", False)),
        alert_on_quality_fail=bool(raw.get("alert_on_quality_fail", False)),
        runbook_url=raw.get("runbook_url"),
        owners=_as_list(raw.get("owners")),
        groups=_as_list(raw.get("groups")),
        tags=_str_map(raw.get("tags"), "operations.tags"),
    )


def normalize_access_contract(value: Any) -> Optional[AccessContract]:
    """Normaliza dict/YAML em ``AccessContract``."""
    if value is None or isinstance(value, AccessContract):
        return value
    raw = _require_mapping(value, "access")
    grants = []
    for item in raw.get("grants", []) or []:
        grant_raw = _require_mapping(item, "access.grants[]")
        principal = str(grant_raw.get("principal") or "").strip()
        privileges = [priv.upper() for priv in _as_list(grant_raw.get("privileges"))]
        if not principal or not privileges:
            raise ValueError("access.grants[] requer principal e privileges")
        grants.append(AccessGrant(principal=principal, privileges=privileges))
    row_filters = []
    for item in raw.get("row_filters", []) or []:
        filter_raw = _require_mapping(item, "access.row_filters[]")
        name = str(filter_raw.get("name") or "").strip()
        function = str(filter_raw.get("function") or "").strip()
        columns = _as_list(filter_raw.get("columns"))
        if not name or not function or not columns:
            raise ValueError("access.row_filters[] requer name, function e columns")
        applies_to = filter_raw.get("applies_to", {}) or {}
        row_filters.append(
            RowFilter(
                name=name,
                function=function,
                columns=columns,
                principals=_as_list(_require_mapping(applies_to, "access.row_filters[].applies_to").get("principals")),
            )
        )
    column_masks = []
    for item in raw.get("column_masks", []) or []:
        mask_raw = _require_mapping(item, "access.column_masks[]")
        column = str(mask_raw.get("column") or "").strip()
        function = str(mask_raw.get("function") or "").strip()
        if not column or not function:
            raise ValueError("access.column_masks[] requer column e function")
        applies_to = mask_raw.get("applies_to", {}) or {}
        column_masks.append(
            ColumnMask(
                column=column,
                function=function,
                using_columns=_as_list(mask_raw.get("using_columns")),
                principals=_as_list(_require_mapping(applies_to, "access.column_masks[].applies_to").get("principals")),
            )
        )
    return AccessContract(
        mode=_enum(raw.get("mode", "apply"), VALID_ACCESS_MODES, "access.mode", "apply"),
        on_drift=_enum(
            raw.get("on_drift", "warn"),
            VALID_ACCESS_DRIFT_POLICIES,
            "access.on_drift",
            "warn",
        ),
        revoke_unmanaged=bool(raw.get("revoke_unmanaged", False)),
        grants=grants,
        row_filters=row_filters,
        column_masks=column_masks,
    )


def annotation_sql_preview(target_table: str, contract: Optional[AnnotationsContract]) -> List[str]:
    """Retorna SQL previsto para annotations, sem executar."""
    if not contract:
        return []
    return [step["sql"] for step in _annotation_steps(target_table, contract)]


def access_sql_preview(target_table: str, contract: Optional[AccessContract]) -> List[str]:
    """Retorna SQL previsto para access, sem executar."""
    if not contract:
        return []
    return [step["sql"] for step in _access_steps(target_table, contract)]


def governance_referenced_columns(
    annotations: Optional[AnnotationsContract],
    access: Optional[AccessContract],
) -> Dict[str, List[str]]:
    """Lista colunas referenciadas pelos contratos de governanca."""
    references = {
        "annotations": sorted(annotations.columns) if annotations else [],
        "row_filters": sorted(
            {column for row_filter in (access.row_filters if access else []) for column in row_filter.columns}
        ),
        "column_masks": sorted(
            {
                column
                for mask in (access.column_masks if access else [])
                for column in [mask.column, *mask.using_columns]
            }
        ),
    }
    references["all"] = sorted(
        set(references["annotations"]) | set(references["row_filters"]) | set(references["column_masks"])
    )
    return references


def validate_governance_contract(
    target_table: str,
    annotations: Optional[AnnotationsContract],
    access: Optional[AccessContract],
    existing_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Valida governanca contra o schema real ou informado da tabela alvo."""
    issues = []
    references = governance_referenced_columns(annotations, access)
    try:
        columns = (
            set(existing_columns)
            if existing_columns is not None
            else {field.name for field in spark.read.table(target_table).schema.fields}
        )
    except Exception as exc:
        return {
            "status": "FAILED",
            "target_table": target_table,
            "references": references,
            "issues": [
                {
                    "severity": "fail",
                    "scope": "table",
                    "object": target_table,
                    "message": f"Nao foi possivel ler schema da tabela alvo: {exc}",
                }
            ],
        }

    for scope, referenced_columns in references.items():
        if scope == "all":
            continue
        missing = sorted(set(referenced_columns) - columns)
        for column in missing:
            issues.append(
                {
                    "severity": "fail",
                    "scope": scope,
                    "object": column,
                    "message": f"Coluna {column!r} referenciada em {scope} nao existe em {target_table}",
                }
            )
    return {
        "status": "FAILED" if issues else "SUCCESS",
        "target_table": target_table,
        "references": references,
        "issues": issues,
    }


def _tag_sql(tags: Dict[str, str]) -> str:
    return ", ".join(f"{sql_lit(key)} = {sql_lit(value)}" for key, value in tags.items())


def _qualified_function(function_name: str) -> str:
    return ".".join(q(part) for part in function_name.split("."))


def _alias_tags(aliases: List[str]) -> Dict[str, str]:
    return {f"alias_{idx}": alias for idx, alias in enumerate(aliases, start=1)}


def _deprecated_tags(deprecated: Optional[DeprecatedAnnotation]) -> Dict[str, str]:
    if not deprecated:
        return {}
    tags = {"deprecated": "true"}
    if deprecated.since:
        tags["deprecated_since"] = deprecated.since
    if deprecated.replacement:
        tags["deprecated_replacement"] = deprecated.replacement
    if deprecated.removal_date:
        tags["deprecated_removal_date"] = deprecated.removal_date
    return tags


def _pii_tags(pii: Optional[PiiAnnotation]) -> Dict[str, str]:
    if not pii:
        return {}
    return {
        "pii": str(pii.enabled).lower(),
        "pii_type": pii.type,
        "sensitivity": pii.sensitivity,
    }


def _annotation_steps(target_table: str, contract: AnnotationsContract) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    if contract.table.description:
        steps.append(
            {
                "annotation_scope": "table",
                "annotation_type": "description",
                "column_name": None,
                "key": "description",
                "value": contract.table.description,
                "sql": f"COMMENT ON TABLE {qt(target_table)} IS {sql_lit(contract.table.description)}",
            }
        )
    table_tags = {
        **contract.table.tags,
        **_alias_tags(contract.table.aliases),
        **_deprecated_tags(contract.table.deprecated),
    }
    if table_tags:
        steps.append(
            {
                "annotation_scope": "table",
                "annotation_type": "tags",
                "column_name": None,
                "key": "tags",
                "value": to_json(table_tags),
                "sql": f"ALTER TABLE {qt(target_table)} SET TAGS ({_tag_sql(table_tags)})",
            }
        )
    for column, annotation in contract.columns.items():
        if annotation.description:
            steps.append(
                {
                    "annotation_scope": "column",
                    "annotation_type": "description",
                    "column_name": column,
                    "key": "description",
                    "value": annotation.description,
                    "sql": (
                        f"ALTER TABLE {qt(target_table)} ALTER COLUMN {q(column)} "
                        f"COMMENT {sql_lit(annotation.description)}"
                    ),
                }
            )
        column_tags = {
            **annotation.tags,
            **_alias_tags(annotation.aliases),
            **_pii_tags(annotation.pii),
            **_deprecated_tags(annotation.deprecated),
        }
        if column_tags:
            steps.append(
                {
                    "annotation_scope": "column",
                    "annotation_type": "tags",
                    "column_name": column,
                    "key": "tags",
                    "value": to_json(column_tags),
                    "sql": (
                        f"ALTER TABLE {qt(target_table)} ALTER COLUMN {q(column)} "
                        f"SET TAGS ({_tag_sql(column_tags)})"
                    ),
                }
            )
    return steps


def _access_steps(target_table: str, contract: AccessContract) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    for grant in contract.grants:
        privileges = ", ".join(grant.privileges)
        steps.append(
            {
                "access_type": "grant",
                "principal": grant.principal,
                "privilege": privileges,
                "object_name": target_table,
                "sql": f"GRANT {privileges} ON TABLE {qt(target_table)} TO {q(grant.principal)}",
            }
        )
    for row_filter in contract.row_filters:
        columns = ", ".join(q(column) for column in row_filter.columns)
        steps.append(
            {
                "access_type": "row_filter",
                "principal": "|".join(row_filter.principals),
                "privilege": "ROW_FILTER",
                "object_name": row_filter.name,
                "sql": (
                    f"ALTER TABLE {qt(target_table)} SET ROW FILTER "
                    f"{_qualified_function(row_filter.function)} ON ({columns})"
                ),
            }
        )
    for mask in contract.column_masks:
        using = ""
        if mask.using_columns:
            using = " USING COLUMNS (" + ", ".join(q(column) for column in mask.using_columns) + ")"
        steps.append(
            {
                "access_type": "column_mask",
                "principal": "|".join(mask.principals),
                "privilege": "COLUMN_MASK",
                "object_name": mask.column,
                "sql": (
                    f"ALTER TABLE {qt(target_table)} ALTER COLUMN {q(mask.column)} "
                    f"SET MASK {_qualified_function(mask.function)}{using}"
                ),
            }
        )
    return steps


def _execute_step(sql: str) -> None:
    spark.sql(sql)


def apply_annotations_contract(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    contract: Optional[AnnotationsContract],
    log_entries,
) -> Dict[str, Any]:
    """Aplica comments/tags e audita em ctrl_ingestion_annotations."""
    if not contract:
        return {"status": "NOT_CONFIGURED", "applied": 0, "failed": 0, "sql_preview": []}
    steps = _annotation_steps(target_table, contract)
    result = {"status": "SUCCESS", "applied": 0, "failed": 0, "sql_preview": [s["sql"] for s in steps]}
    entries = []
    if contract.policy == "ignore":
        for step in steps:
            entries.append({**step, "status": "IGNORED", "error_message": None})
        log_entries(tables, run_id, target_table, entries)
        result["status"] = "IGNORED"
        return result
    for step in steps:
        try:
            _execute_step(step["sql"])
            result["applied"] += 1
            entries.append({**step, "status": "APPLIED", "error_message": None})
        except Exception as exc:
            result["failed"] += 1
            status = "FAILED" if contract.policy == "fail" else "WARNED"
            entries.append({**step, "status": status, "error_message": str(exc)})
            if contract.policy == "fail":
                log_entries(tables, run_id, target_table, entries)
                raise
            logger.warning(f"Falha ao aplicar annotation em {target_table}: {exc}")
    if result["failed"]:
        result["status"] = "WARNED"
    log_entries(tables, run_id, target_table, entries)
    return result


def record_operations_contract(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    contract: Optional[OperationsContract],
    log_entry,
) -> Dict[str, Any]:
    """Registra contrato operacional para consumo por dashboards/alertas."""
    if not contract:
        return {"status": "NOT_CONFIGURED"}
    payload = {
        "criticality": contract.criticality,
        "expected_frequency": contract.expected_frequency,
        "freshness_sla_minutes": contract.freshness_sla_minutes,
        "alert_on_failure": contract.alert_on_failure,
        "alert_on_quality_fail": contract.alert_on_quality_fail,
        "runbook_url": contract.runbook_url,
        "owners_json": to_json(contract.owners),
        "groups_json": to_json(contract.groups),
        "tags_json": to_json(contract.tags),
        "status": "RECORDED",
        "recorded_at_utc": utc_now_str(),
    }
    log_entry(tables, run_id, target_table, payload)
    return {"status": "RECORDED", "criticality": contract.criticality}


def apply_access_contract(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    contract: Optional[AccessContract],
    log_entries,
) -> Dict[str, Any]:
    """Aplica grants, row filters e masks declarados no contrato de acesso."""
    if not contract:
        return {"status": "NOT_CONFIGURED", "applied": 0, "failed": 0, "sql_preview": []}
    steps = _access_steps(target_table, contract)
    result = {"status": "SUCCESS", "applied": 0, "failed": 0, "sql_preview": [s["sql"] for s in steps]}
    entries = []
    if contract.mode == "ignore":
        for step in steps:
            entries.append({**step, "status": "IGNORED", "error_message": None, "previous_value": None})
        log_entries(tables, run_id, target_table, entries)
        result["status"] = "IGNORED"
        return result
    if contract.mode == "validate_only":
        for step in steps:
            entries.append({**step, "status": "VALIDATED", "error_message": None, "previous_value": None})
        log_entries(tables, run_id, target_table, entries)
        result["status"] = "VALIDATED"
        return result
    for step in steps:
        try:
            _execute_step(step["sql"])
            result["applied"] += 1
            entries.append({**step, "status": "APPLIED", "error_message": None, "previous_value": None})
        except Exception as exc:
            result["failed"] += 1
            status = "FAILED" if contract.on_drift == "fail" else "WARNED"
            entries.append({**step, "status": status, "error_message": str(exc), "previous_value": None})
            if contract.on_drift == "fail":
                log_entries(tables, run_id, target_table, entries)
                raise
            logger.warning(f"Falha ao aplicar access em {target_table}: {exc}")
    if result["failed"]:
        result["status"] = "WARNED"
    log_entries(tables, run_id, target_table, entries)
    return result
