"""Contract bundle and governance orchestration."""
from __future__ import annotations

from typing import Any, Dict, Optional

from ._sql import new_run_id, to_json, utc_now_ts
from .config import CTRL_SCHEMA_VERSION, FRAMEWORK_VERSION
from .contract_bundle import ContractBundle, governance_check, governance_preview, load_contract_bundle
from .governance import (
    access_sql_preview,
    apply_access_contract,
    apply_annotations_contract,
    record_operations_contract,
    validate_governance_contract,
)
from .ingestion import ingest_plan
from .plan import target_full_table_name, target_schema_name
from .state import (
    ensure_ctrl_tables,
    log_access_entries,
    log_annotation_entries,
    log_operations_contract,
)


def ingest_bundle(path: str | ContractBundle, *, raise_on_failure: bool = True) -> Dict[str, Any]:
    """Load a split contract bundle and execute its ingestion plan."""
    bundle = path if isinstance(path, ContractBundle) else load_contract_bundle(path)
    return ingest_plan(bundle.ingestion, raise_on_failure=raise_on_failure)


def apply_governance_bundle(
    path: str,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Aplica operations e annotations de um bundle sem reprocessar dados.

    Access tem ciclo proprio por normalmente exigir permissoes elevadas. Use
    ``apply_access_bundle`` para grants, row filters e column masks.
    """
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
