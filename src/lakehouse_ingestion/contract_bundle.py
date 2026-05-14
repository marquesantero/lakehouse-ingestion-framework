"""Loader de contratos separados por responsabilidade."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from .governance import (
    AccessContract,
    AnnotationsContract,
    OperationsContract,
    access_drift_report,
    access_sql_preview,
    annotation_sql_preview,
    validate_governance_contract,
)
from .plan import (
    IngestionPlan,
    build_plan_from_kwargs,
    target_full_table_name,
    target_schema_name,
)

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


@dataclass(frozen=True)
class ContractBundle:
    """Pacote logico de contratos de uma tabela."""

    ingestion: IngestionPlan
    annotations: Optional[AnnotationsContract] = None
    operations: Optional[OperationsContract] = None
    access: Optional[AccessContract] = None
    metadata: dict[str, dict[str, Any]] | None = None
    paths: dict[str, str] | None = None


def _strip_metadata(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    clean = dict(payload)
    metadata = clean.pop("_metadata", {}) or {}
    if not isinstance(metadata, dict):
        raise ValueError("_metadata deve ser um objeto/dict")
    return clean, metadata


def _target_tuple(payload: dict[str, Any]) -> Optional[tuple[Optional[str], Optional[str], Optional[str]]]:
    target = payload.get("target")
    if target is None:
        return None
    if not isinstance(target, dict):
        raise ValueError("target deve ser um objeto/dict")
    table = target.get("table")
    if not table:
        raise ValueError("target.table e obrigatorio quando target for informado")
    return (
        str(target.get("catalog")).strip() if target.get("catalog") else None,
        str(target.get("schema")).strip() if target.get("schema") else None,
        str(table).strip(),
    )


def _validate_target_compatibility(
    kind: str,
    payload: dict[str, Any],
    plan: IngestionPlan,
) -> None:
    declared = _target_tuple(payload)
    if declared is None:
        return
    catalog, schema, table = declared
    expected = (plan.catalog, target_schema_name(plan), plan.target_table)
    if catalog and catalog != expected[0]:
        raise ValueError(f"{kind}.target.catalog={catalog!r} diverge de ingestion.catalog={expected[0]!r}")
    if schema and schema != expected[1]:
        raise ValueError(f"{kind}.target.schema={schema!r} diverge do target schema físico={expected[1]!r}")
    if table != expected[2]:
        raise ValueError(f"{kind}.target.table={table!r} diverge de ingestion.target_table={expected[2]!r}")


def _metadata_validation_warnings(metadata: dict[str, dict[str, Any]]) -> list[str]:
    warnings = []
    majors = {}
    for name, content in metadata.items():
        version = content.get("contract_version")
        if not version:
            continue
        version_text = str(version)
        if not _SEMVER_RE.match(version_text):
            warnings.append(f"{name}._metadata.contract_version nao esta em formato MAJOR.MINOR.PATCH: {version_text}")
            continue
        majors[name] = version_text.split(".", 1)[0]
    if len(set(majors.values())) > 1:
        warnings.append(f"major version divergente entre arquivos do bundle: {majors}")
    return warnings


def _load_structured(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        try:
            import yaml  # type: ignore
        except Exception as exc:
            raise RuntimeError("Carga de YAML requer PyYAML instalado") from exc
        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} deve conter um objeto")
    return payload


def _candidate_paths(base: Path, suffix: str) -> Iterable[Path]:
    if base.is_file():
        stem = base.name
        if ".ingestion." in stem:
            yield base.with_name(stem.replace(".ingestion.", f".{suffix}.", 1))
        return
    yield base.with_suffix(f".{suffix}.yaml")
    yield base.with_suffix(f".{suffix}.yml")
    yield base.with_suffix(f".{suffix}.json")


def _first_existing(base: Path, suffix: str) -> Optional[Path]:
    for candidate in _candidate_paths(base, suffix):
        if candidate.exists():
            return candidate
    return None


def load_contract_bundle(path: str | Path) -> ContractBundle:
    """Carrega ``*.ingestion.yaml`` e arquivos irmaos opcionais.

    Exemplo para ``contracts/gold/gd_orders_daily``:

    - ``gd_orders_daily.ingestion.yaml`` ou ``gd_orders_daily.yaml``
    - ``gd_orders_daily.annotations.yaml``
    - ``gd_orders_daily.operations.yaml``
    - ``gd_orders_daily.access.yaml``
    """
    base = Path(path)
    ingestion_path = base if base.is_file() else _first_existing(base, "ingestion")
    if ingestion_path is None:
        default_path = base.with_suffix(".yaml")
        ingestion_path = default_path if default_path.exists() else None
    if ingestion_path is None:
        raise FileNotFoundError(f"Contrato de ingestao nao encontrado para {base}")

    ingestion_payload, ingestion_metadata = _strip_metadata(_load_structured(ingestion_path))
    annotations_path = _first_existing(ingestion_path, "annotations")
    operations_path = _first_existing(ingestion_path, "operations")
    access_path = _first_existing(ingestion_path, "access")
    metadata = {"ingestion": ingestion_metadata}
    paths = {"ingestion": str(ingestion_path)}

    annotations_payload = operations_payload = access_payload = None
    if annotations_path:
        annotations_payload, annotations_metadata = _strip_metadata(_load_structured(annotations_path))
        ingestion_payload["annotations"] = annotations_payload
        metadata["annotations"] = annotations_metadata
        paths["annotations"] = str(annotations_path)
    if operations_path:
        operations_payload, operations_metadata = _strip_metadata(_load_structured(operations_path))
        ingestion_payload["operations"] = operations_payload
        metadata["operations"] = operations_metadata
        paths["operations"] = str(operations_path)
    if access_path:
        access_payload, access_metadata = _strip_metadata(_load_structured(access_path))
        ingestion_payload["access"] = access_payload
        metadata["access"] = access_metadata
        paths["access"] = str(access_path)

    plan = build_plan_from_kwargs(**ingestion_payload)
    if annotations_payload is not None:
        _validate_target_compatibility("annotations", annotations_payload, plan)
    if operations_payload is not None:
        _validate_target_compatibility("operations", operations_payload, plan)
    if access_payload is not None:
        _validate_target_compatibility("access", access_payload, plan)
    return ContractBundle(
        ingestion=plan,
        annotations=plan.annotations,
        operations=plan.operations,
        access=plan.access,
        metadata=metadata,
        paths=paths,
    )


def governance_preview(bundle: ContractBundle) -> dict[str, Any]:
    """Retorna preview executavel das acoes de governanca do bundle."""
    plan = bundle.ingestion
    target = target_full_table_name(plan)
    return {
        "target_table": target,
        "annotations_sql": annotation_sql_preview(target, bundle.annotations),
        "access_sql": access_sql_preview(target, bundle.access),
        "operations": bundle.operations,
        "metadata": bundle.metadata or {},
        "metadata_warnings": contract_metadata_warnings(bundle),
        "paths": bundle.paths or {},
    }


def contract_metadata_warnings(bundle: ContractBundle) -> list[str]:
    """Aponta inconsistencias entre `_metadata` dos arquivos do bundle."""
    metadata = bundle.metadata or {}
    warnings = _metadata_validation_warnings(metadata)
    versions = {
        name: content.get("contract_version")
        for name, content in metadata.items()
        if content.get("contract_version")
    }
    if len(set(versions.values())) > 1:
        warnings.append(f"contract_version divergente entre arquivos do bundle: {versions}")
    return warnings


def governance_check(bundle: ContractBundle) -> dict[str, Any]:
    """Valida governanca contra o catalogo alvo sem aplicar alteracoes."""
    plan = bundle.ingestion
    target = target_full_table_name(plan)
    validation = validate_governance_contract(target, bundle.annotations, bundle.access)
    access_drift = access_drift_report(target, bundle.access)
    metadata_warnings = contract_metadata_warnings(bundle)
    access_drift_failed = (
        access_drift["status"] == "FAILED"
        or (access_drift["status"] == "DRIFTED" and bundle.access is not None and bundle.access.on_drift == "fail")
    )
    status = "FAILED" if validation["status"] == "FAILED" or access_drift_failed else "SUCCESS"
    if status == "SUCCESS" and access_drift["status"] == "DRIFTED":
        status = "WARNED"
    if status == "SUCCESS" and metadata_warnings:
        status = "WARNED"
    return {
        "status": status,
        "target_table": target,
        "validation": validation,
        "access_drift": access_drift,
        "metadata_warnings": metadata_warnings,
        "preview": governance_preview(bundle),
    }
