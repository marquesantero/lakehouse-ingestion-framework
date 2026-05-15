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
from ._uc_capabilities import capability_issues
from .config import (
    AccessDriftPolicy,
    AccessMode,
    GovernanceFailurePolicy,
    VALID_ACCESS_DRIFT_POLICIES,
    VALID_ACCESS_MODES,
    VALID_ACCESS_PRIVILEGES,
    VALID_CRITICALITY_LEVELS,
    VALID_EXPECTED_FREQUENCIES,
    VALID_GOVERNANCE_FAILURE_POLICIES,
    VALID_PII_TYPES,
    VALID_SENSITIVITY_LEVELS,
)

logger = logging.getLogger("contractforge")


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

    business_owner: Optional[str] = None
    technical_owner: Optional[str] = None
    steward: Optional[str] = None
    support_group: Optional[str] = None
    escalation_group: Optional[str] = None
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


def _non_empty_list(value: Any, field: str) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.split("|")
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [value]
    items = [str(item).strip() for item in raw_items]
    if any(not item for item in items):
        raise ValueError(f"{field} nao pode conter valor vazio")
    return items


def _required_non_empty_string(value: Any, field: str) -> str:
    parsed = str(value or "").strip()
    if not parsed:
        raise ValueError(f"{field} nao pode ser vazio")
    return parsed


def _optional_non_empty_string(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    return _required_non_empty_string(value, field)


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


def _qualified_name(value: Any, field: str) -> str:
    name = _required_non_empty_string(value, field)
    parts = [part.strip() for part in name.split(".")]
    if len(parts) < 3 or any(not part for part in parts):
        raise ValueError(f"{field} deve ser qualificado, por exemplo catalog.schema.funcao")
    return name


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
    since = _required_non_empty_string(raw.get("since"), f"{field}.since")
    replacement = _required_non_empty_string(raw.get("replacement"), f"{field}.replacement")
    return DeprecatedAnnotation(
        since=since,
        replacement=replacement,
        removal_date=_optional_non_empty_string(raw.get("removal_date"), f"{field}.removal_date"),
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
        description=_optional_non_empty_string(table_raw.get("description"), "annotations.table.description"),
        aliases=_non_empty_list(table_raw.get("aliases"), "annotations.table.aliases"),
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
            description=_optional_non_empty_string(
                column_raw.get("description"),
                f"annotations.columns.{column_name}.description",
            ),
            aliases=_non_empty_list(column_raw.get("aliases"), f"annotations.columns.{column_name}.aliases"),
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
    ownership = _require_mapping(raw.get("ownership", {}), "operations.ownership")
    operations_raw = _require_mapping(raw.get("operations", raw), "operations.operations")
    criticality = operations_raw.get("criticality")
    if criticality is not None:
        criticality = _enum(
            criticality,
            VALID_CRITICALITY_LEVELS,
            "operations.criticality",
        )
    expected_frequency = operations_raw.get("expected_frequency")
    if expected_frequency is not None:
        expected_frequency = _enum(
            expected_frequency,
            VALID_EXPECTED_FREQUENCIES,
            "operations.expected_frequency",
        )
    return OperationsContract(
        business_owner=_optional_non_empty_string(ownership.get("business_owner"), "operations.ownership.business_owner"),
        technical_owner=_optional_non_empty_string(
            ownership.get("technical_owner"),
            "operations.ownership.technical_owner",
        ),
        steward=_optional_non_empty_string(ownership.get("steward"), "operations.ownership.steward"),
        support_group=_optional_non_empty_string(ownership.get("support_group"), "operations.ownership.support_group"),
        escalation_group=_optional_non_empty_string(
            ownership.get("escalation_group"),
            "operations.ownership.escalation_group",
        ),
        criticality=criticality,
        expected_frequency=expected_frequency,
        freshness_sla_minutes=_positive_int(
            operations_raw.get("freshness_sla_minutes"),
            "operations.freshness_sla_minutes",
        ),
        alert_on_failure=bool(operations_raw.get("alert_on_failure", False)),
        alert_on_quality_fail=bool(operations_raw.get("alert_on_quality_fail", False)),
        runbook_url=_optional_non_empty_string(operations_raw.get("runbook_url"), "operations.runbook_url"),
        owners=_as_list(operations_raw.get("owners")),
        groups=_as_list(operations_raw.get("groups")),
        tags=_str_map(operations_raw.get("tags"), "operations.tags"),
    )


def normalize_access_contract(value: Any) -> Optional[AccessContract]:
    """Normaliza dict/YAML em ``AccessContract``."""
    if value is None or isinstance(value, AccessContract):
        return value
    raw = _require_mapping(value, "access")
    policy = _require_mapping(raw.get("access_policy", {}), "access.access_policy")
    grants = []
    for item in raw.get("grants", []) or []:
        grant_raw = _require_mapping(item, "access.grants[]")
        principal = str(grant_raw.get("principal") or "").strip()
        privileges = [priv.upper() for priv in _as_list(grant_raw.get("privileges"))]
        if not principal or not privileges:
            raise ValueError("access.grants[] requer principal e privileges")
        invalid = sorted(set(privileges) - VALID_ACCESS_PRIVILEGES)
        if invalid:
            raise ValueError(f"access.grants[].privileges contem valores invalidos: {invalid}")
        grants.append(AccessGrant(principal=principal, privileges=privileges))
    row_filters = []
    for item in raw.get("row_filters", []) or []:
        filter_raw = _require_mapping(item, "access.row_filters[]")
        name = str(filter_raw.get("name") or "").strip()
        function = _qualified_name(filter_raw.get("function"), "access.row_filters[].function")
        columns = _as_list(filter_raw.get("columns"))
        if not name or not columns:
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
    masks_raw = raw.get("column_masks", []) or []
    if isinstance(masks_raw, dict):
        masks_iter = [{**_require_mapping(config, f"access.column_masks.{column}"), "column": column} for column, config in masks_raw.items()]
    else:
        masks_iter = masks_raw
    for item in masks_iter:
        mask_raw = _require_mapping(item, "access.column_masks[]")
        column = str(mask_raw.get("column") or "").strip()
        function = _qualified_name(mask_raw.get("function"), "access.column_masks[].function")
        if not column:
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
        mode=_enum(policy.get("mode", raw.get("mode", "apply")), VALID_ACCESS_MODES, "access.mode", "apply"),
        on_drift=_enum(
            policy.get("on_drift", raw.get("on_drift", "warn")),
            VALID_ACCESS_DRIFT_POLICIES,
            "access.on_drift",
            "warn",
        ),
        revoke_unmanaged=bool(policy.get("revoke_unmanaged", raw.get("revoke_unmanaged", False))),
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


def _row_value(row: Any, *names: str) -> Any:
    """Le campo Spark Row tolerando variacoes de casing/nome por runtime."""
    for name in names:
        try:
            return row[name]
        except Exception:
            pass
    data = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    lower = {str(key).lower(): value for key, value in data.items()}
    for name in names:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def _current_grants(target_table: str) -> set[tuple[str, str]]:
    rows = spark.sql(f"SHOW GRANTS ON TABLE {qt(target_table)}").collect()
    grants = set()
    for row in rows:
        principal = _row_value(row, "Principal", "principal", "grantee")
        privilege = _row_value(row, "ActionType", "actionType", "Privilege", "privilege")
        if principal and privilege:
            grants.add((str(principal), str(privilege).upper()))
    return grants


def _declared_grants(contract: Optional[AccessContract]) -> set[tuple[str, str]]:
    if not contract:
        return set()
    return {
        (grant.principal, privilege.upper())
        for grant in contract.grants
        for privilege in grant.privileges
    }


def access_drift_report(
    target_table: str,
    contract: Optional[AccessContract],
    current_grants: Optional[set[tuple[str, str]]] = None,
) -> Dict[str, Any]:
    """Compara grants declarados com grants atuais do catalogo."""
    if not contract:
        return {
            "status": "NOT_CONFIGURED",
            "target_table": target_table,
            "declared_grants": [],
            "current_grants": [],
            "missing_grants": [],
            "unmanaged_grants": [],
            "issues": [],
        }
    declared = _declared_grants(contract)
    try:
        current = current_grants if current_grants is not None else _current_grants(target_table)
    except Exception as exc:
        return {
            "status": "FAILED",
            "target_table": target_table,
            "declared_grants": sorted(declared),
            "current_grants": [],
            "missing_grants": [],
            "unmanaged_grants": [],
            "issues": [
                {
                    "severity": "fail",
                    "scope": "access",
                    "object": target_table,
                    "message": f"Nao foi possivel ler grants da tabela alvo: {exc}",
                }
            ],
        }
    missing = sorted(declared - current)
    unmanaged = sorted(current - declared)
    drift_severity = "fail" if contract.on_drift == "fail" else "warn"
    issues = [
        {
            "severity": drift_severity,
            "scope": "grant",
            "object": f"{principal}:{privilege}",
            "message": f"Grant declarado ausente: {privilege} para {principal}",
        }
        for principal, privilege in missing
    ]
    if contract.revoke_unmanaged:
        issues.extend(
            {
                "severity": drift_severity,
                "scope": "grant",
                "object": f"{principal}:{privilege}",
                "message": (
                    f"Grant atual nao declarado "
                    f"{'sera revogado' if contract.on_drift == 'reconcile' else 'foi detectado'}: "
                    f"{privilege} de {principal}"
                ),
            }
            for principal, privilege in unmanaged
        )
    return {
        "status": "DRIFTED" if missing or (contract.revoke_unmanaged and unmanaged) else "IN_SYNC",
        "target_table": target_table,
        "declared_grants": sorted(declared),
        "current_grants": sorted(current),
        "missing_grants": missing,
        "unmanaged_grants": unmanaged,
        "issues": issues,
    }


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
    requirements: List[tuple[str, str, str, str]] = []
    if annotations:
        if annotations.table.tags or annotations.table.aliases or annotations.table.deprecated:
            requirements.append(
                (
                    "table_tags",
                    "annotations",
                    "table.tags",
                    "fail" if annotations.policy == "fail" else "warn",
                )
            )
        for column, annotation in annotations.columns.items():
            if annotation.tags or annotation.aliases or annotation.pii or annotation.deprecated:
                requirements.append(
                    (
                        "column_tags",
                        "annotations",
                        column,
                        "fail" if annotations.policy == "fail" else "warn",
                    )
                )
    if access:
        if access.row_filters:
            requirements.append(("row_filters", "access", "row_filters", "fail"))
        if access.column_masks:
            requirements.append(("column_masks", "access", "column_masks", "fail"))
    issues.extend(capability_issues(target_table, requirements))
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

    if annotations:
        contains_pii = annotations.table.tags.get("contains_pii", "").lower() == "true"
        pii_columns = sorted(
            column
            for column, annotation in annotations.columns.items()
            if annotation.pii and annotation.pii.enabled
        )
        if contains_pii and not pii_columns:
            issues.append(
                {
                    "severity": "fail",
                    "scope": "annotations",
                    "object": "table.tags.contains_pii",
                    "message": "contains_pii=true exige pelo menos uma coluna com pii.enabled=true",
                }
            )
        for column in pii_columns:
            if not annotations.columns[column].description:
                issues.append(
                    {
                        "severity": "warn",
                        "scope": "annotations",
                        "object": column,
                        "message": f"Coluna PII {column!r} deveria ter description declarada",
                    }
                )

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
        "status": "FAILED" if any(issue["severity"] == "fail" for issue in issues) else "SUCCESS",
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
        for privilege in grant.privileges:
            steps.append(
                {
                    "access_type": "grant",
                    "principal": grant.principal,
                    "privilege": privilege,
                    "column_name": None,
                    "function_name": None,
                    "object_name": target_table,
                    "new_value": "GRANTED",
                    "mode": contract.mode,
                    "drift_policy": contract.on_drift,
                    "revoke_unmanaged": contract.revoke_unmanaged,
                    "sql": f"GRANT {privilege} ON TABLE {qt(target_table)} TO {q(grant.principal)}",
                }
            )
    for row_filter in contract.row_filters:
        columns = ", ".join(q(column) for column in row_filter.columns)
        steps.append(
            {
                "access_type": "row_filter",
                "principal": "|".join(row_filter.principals),
                "privilege": "ROW_FILTER",
                "column_name": "|".join(row_filter.columns),
                "function_name": row_filter.function,
                "object_name": row_filter.name,
                "new_value": row_filter.function,
                "mode": contract.mode,
                "drift_policy": contract.on_drift,
                "revoke_unmanaged": contract.revoke_unmanaged,
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
                "column_name": mask.column,
                "function_name": mask.function,
                "object_name": mask.column,
                "new_value": mask.function,
                "mode": contract.mode,
                "drift_policy": contract.on_drift,
                "revoke_unmanaged": contract.revoke_unmanaged,
                "sql": (
                    f"ALTER TABLE {qt(target_table)} ALTER COLUMN {q(mask.column)} "
                    f"SET MASK {_qualified_function(mask.function)}{using}"
                ),
            }
        )
    return steps


def _revoke_grant_steps(
    target_table: str,
    grants: Iterable[tuple[str, str]],
    contract: AccessContract,
) -> List[Dict[str, Any]]:
    steps = []
    for principal, privilege in grants:
        steps.append(
            {
                "access_type": "revoke",
                "principal": principal,
                "privilege": privilege,
                "column_name": None,
                "function_name": None,
                "object_name": target_table,
                "sql": f"REVOKE {privilege} ON TABLE {qt(target_table)} FROM {q(principal)}",
                "previous_value": "GRANTED",
                "new_value": "REVOKED",
                "mode": contract.mode,
                "drift_policy": contract.on_drift,
                "revoke_unmanaged": contract.revoke_unmanaged,
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
            logger.warning("Falha ao aplicar annotation em %s: %s", target_table, exc)
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
    ownership = {
        "business_owner": contract.business_owner,
        "technical_owner": contract.technical_owner,
        "steward": contract.steward,
        "owners": contract.owners,
    }
    groups = {
        "support_group": contract.support_group,
        "escalation_group": contract.escalation_group,
        "groups": contract.groups,
    }
    payload = {
        "criticality": contract.criticality,
        "expected_frequency": contract.expected_frequency,
        "freshness_sla_minutes": contract.freshness_sla_minutes,
        "alert_on_failure": contract.alert_on_failure,
        "alert_on_quality_fail": contract.alert_on_quality_fail,
        "runbook_url": contract.runbook_url,
        "ownership_json": to_json({"ownership": ownership, "groups": groups}),
        "owners_json": to_json(ownership),
        "groups_json": to_json(groups),
        "tags_json": to_json(contract.tags),
        "status": "RECORDED",
        "recorded_at_utc": utc_now_str(),
    }
    log_entry(tables, run_id, target_table, payload)
    return {
        "status": "RECORDED",
        "criticality": contract.criticality,
        "ownership": ownership,
        "operations": {
            "expected_frequency": contract.expected_frequency,
            "freshness_sla_minutes": contract.freshness_sla_minutes,
            "alert_on_failure": contract.alert_on_failure,
            "alert_on_quality_fail": contract.alert_on_quality_fail,
            "runbook_url": contract.runbook_url,
            "tags": contract.tags,
        },
    }


def apply_access_contract(
    tables: Dict[str, str],
    run_id: str,
    target_table: str,
    contract: Optional[AccessContract],
    log_entries,
    *,
    allow_revoke_unmanaged: bool = False,
) -> Dict[str, Any]:
    """Aplica grants, row filters e masks declarados no contrato de acesso."""
    if not contract:
        return {"status": "NOT_CONFIGURED", "applied": 0, "failed": 0, "sql_preview": []}
    if contract.revoke_unmanaged and not allow_revoke_unmanaged:
        raise ValueError(
            "access.revoke_unmanaged=true exige confirmacao explicita "
            "no caminho de aplicacao dedicado (--force-revoke)"
        )
    drift = access_drift_report(target_table, contract)
    if drift["status"] == "FAILED" and contract.on_drift == "fail":
        raise ValueError(f"Falha ao calcular drift de access: {to_json(drift['issues'])}")
    if drift["status"] == "DRIFTED" and contract.on_drift == "fail":
        raise ValueError(f"Drift de access detectado: {to_json(drift['issues'])}")
    steps = _access_steps(target_table, contract)
    if contract.revoke_unmanaged and drift["status"] != "FAILED":
        steps.extend(_revoke_grant_steps(target_table, drift["unmanaged_grants"], contract))
    result = {"status": "SUCCESS", "applied": 0, "failed": 0, "sql_preview": [s["sql"] for s in steps]}
    entries = []
    if contract.mode == "ignore":
        for step in steps:
            entries.append({**step, "status": "IGNORED", "error_message": None})
        log_entries(tables, run_id, target_table, entries)
        result["status"] = "IGNORED"
        result["drift"] = drift
        return result
    if contract.mode == "validate_only":
        for step in steps:
            previous_value = "GRANTED" if (step.get("principal"), step.get("privilege")) in drift.get(
                "current_grants", []
            ) else None
            entries.append({**step, "status": "VALIDATED", "error_message": None, "previous_value": previous_value})
        log_entries(tables, run_id, target_table, entries)
        result["status"] = "VALIDATED"
        result["drift"] = drift
        return result
    for step in steps:
        try:
            _execute_step(step["sql"])
            result["applied"] += 1
            previous_value = step.get("previous_value")
            if previous_value is None and (step.get("principal"), step.get("privilege")) in drift.get(
                "current_grants", []
            ):
                previous_value = "GRANTED"
            entries.append({**step, "status": "APPLIED", "error_message": None, "previous_value": previous_value})
        except Exception as exc:
            result["failed"] += 1
            status = "FAILED" if contract.on_drift == "fail" else "WARNED"
            entries.append({**step, "status": status, "error_message": str(exc), "previous_value": None})
            if contract.on_drift == "fail":
                log_entries(tables, run_id, target_table, entries)
                raise
            logger.warning("Falha ao aplicar access em %s: %s", target_table, exc)
    if result["failed"]:
        result["status"] = "WARNED"
    log_entries(tables, run_id, target_table, entries)
    result["drift"] = drift
    return result
