"""Loader de contratos separados por responsabilidade."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from ._sql import full_table_name
from .governance import (
    AccessContract,
    AnnotationsContract,
    OperationsContract,
    access_drift_report,
    access_sql_preview,
    annotation_sql_preview,
    validate_governance_contract,
)
from .plan import IngestionPlan, build_plan_from_kwargs


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
    target = full_table_name(plan.catalog, plan.layer, plan.target_table)
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
    versions = {
        name: content.get("contract_version")
        for name, content in metadata.items()
        if content.get("contract_version")
    }
    if len(set(versions.values())) <= 1:
        return []
    return [f"contract_version divergente entre arquivos do bundle: {versions}"]


def governance_check(bundle: ContractBundle) -> dict[str, Any]:
    """Valida governanca contra o catalogo alvo sem aplicar alteracoes."""
    plan = bundle.ingestion
    target = full_table_name(plan.catalog, plan.layer, plan.target_table)
    validation = validate_governance_contract(target, bundle.annotations, bundle.access)
    access_drift = access_drift_report(target, bundle.access)
    metadata_warnings = contract_metadata_warnings(bundle)
    status = "FAILED" if validation["status"] == "FAILED" or access_drift["status"] == "FAILED" else "SUCCESS"
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
