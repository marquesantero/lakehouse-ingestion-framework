from __future__ import annotations

import json

import pytest

import contractforge.governance as governance_module
from contractforge.contract_bundle import governance_preview, load_contract_bundle
from contractforge.governance import (
    AccessContract,
    AnnotationsContract,
    OperationsContract,
    access_drift_report,
    access_sql_preview,
    annotation_sql_preview,
    apply_access_contract,
    validate_governance_contract,
)
from contractforge.plan import build_plan_from_kwargs


def test_build_plan_accepts_annotations_operations_and_access():
    plan = build_plan_from_kwargs(
        source="raw_orders",
        target_table="gd_orders",
        annotations={
            "policy": "warn",
            "table": {
                "description": "Pedidos consolidados.",
                "aliases": ["orders", "sales orders"],
                "tags": {"domain": "sales", "contains_pii": True},
            },
            "columns": {
                "customer_email": {
                    "description": "Email do cliente.",
                    "pii": {"enabled": True, "type": "email", "sensitivity": "restricted"},
                    "tags": {"confidentiality": "restricted"},
                }
            },
        },
        operations={
            "criticality": "high",
            "expected_frequency": "daily",
            "freshness_sla_minutes": "180",
            "alert_on_failure": True,
            "owners": "data-platform|sales-analytics",
        },
        access={
            "mode": "validate_only",
            "on_drift": "warn",
            "grants": [{"principal": "data-readers", "privileges": "SELECT"}],
        },
    )

    assert isinstance(plan.annotations, AnnotationsContract)
    assert isinstance(plan.operations, OperationsContract)
    assert isinstance(plan.access, AccessContract)
    assert plan.annotations.table.tags["contains_pii"] == "true"
    assert plan.annotations.columns["customer_email"].pii.type == "email"
    assert plan.operations.freshness_sla_minutes == 180
    assert plan.access.grants[0].privileges == ["SELECT"]


def test_governance_rejects_invalid_enums():
    with pytest.raises(ValueError, match="annotations.policy"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            annotations={"policy": "block"},
        )
    with pytest.raises(ValueError, match="operations.criticality"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            operations={"criticality": "urgent"},
        )
    with pytest.raises(ValueError, match="pii.type"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            annotations={"columns": {"email": {"pii": {"type": "mail"}}}},
        )
    with pytest.raises(ValueError, match="access.mode"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            access={"mode": "dry"},
        )
    with pytest.raises(ValueError, match="operations.expected_frequency"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            operations={"expected_frequency": "sometimes"},
        )
    with pytest.raises(ValueError, match="privileges"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            access={"grants": [{"principal": "readers", "privileges": ["DROP"]}]},
        )


def test_governance_sql_preview_generates_catalog_statements():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        annotations={
            "table": {"description": "Tabela de pedidos.", "tags": {"domain": "sales"}},
            "columns": {
                "email": {
                    "description": "Email.",
                    "pii": {"enabled": True, "type": "email", "sensitivity": "restricted"},
                }
            },
        },
        access={
            "grants": [{"principal": "data-readers", "privileges": ["SELECT"]}],
            "row_filters": [
                {
                    "name": "by_region",
                    "function": "main.security.fn_region",
                    "columns": ["region"],
                }
            ],
            "column_masks": [
                {
                    "column": "email",
                    "function": "main.security.mask_email",
                    "using_columns": ["email"],
                }
            ],
        },
    )

    annotations_sql = annotation_sql_preview("main.gold.orders", plan.annotations)
    access_sql = access_sql_preview("main.gold.orders", plan.access)

    assert "COMMENT ON TABLE `main`.`gold`.`orders`" in annotations_sql[0]
    assert any("ALTER COLUMN `email` SET TAGS" in sql for sql in annotations_sql)
    assert access_sql[0] == "GRANT SELECT ON TABLE `main`.`gold`.`orders` TO `data-readers`"
    assert any("SET ROW FILTER `main`.`security`.`fn_region`" in sql for sql in access_sql)
    assert any("ALTER COLUMN `email` SET MASK `main`.`security`.`mask_email`" in sql for sql in access_sql)


def test_load_contract_bundle_reads_split_json_contracts(tmp_path):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps(
            {
                "_metadata": {"contract_version": "1.0.0", "last_updated_by": "data-platform"},
                "source": "silver.orders",
                "target_table": "gd_orders",
                "layer": "gold",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.annotations.json").write_text(
        json.dumps(
            {
                "target": {"catalog": "main", "schema": "gold", "table": "gd_orders"},
                "table": {"description": "Gold orders"},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.operations.json").write_text(
        json.dumps(
            {
                "target": {"catalog": "main", "schema": "gold", "table": "gd_orders"},
                "ownership": {"technical_owner": "data-platform"},
                "operations": {"criticality": "critical", "expected_frequency": "daily"},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.access.json").write_text(
        json.dumps(
            {
                "target": {"catalog": "main", "schema": "gold", "table": "gd_orders"},
                "access_policy": {"mode": "validate_only"},
                "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
                "column_masks": {"email": {"function": "main.security.mask_email"}},
            }
        ),
        encoding="utf-8",
    )

    bundle = load_contract_bundle(base)

    assert bundle.ingestion.target_table == "gd_orders"
    assert bundle.annotations.table.description == "Gold orders"
    assert bundle.operations.criticality == "critical"
    assert bundle.operations.technical_owner == "data-platform"
    assert bundle.access.mode == "validate_only"
    assert bundle.access.column_masks[0].column == "email"
    assert bundle.metadata["ingestion"]["contract_version"] == "1.0.0"
    assert "ingestion" in bundle.paths


def test_load_contract_bundle_rejects_target_mismatch(tmp_path):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.annotations.json").write_text(
        json.dumps({"target": {"schema": "silver", "table": "gd_orders"}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="annotations.target.schema"):
        load_contract_bundle(base)


def test_governance_preview_from_bundle(tmp_path):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.annotations.json").write_text(
        json.dumps({"table": {"description": "Gold orders"}}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.access.json").write_text(
        json.dumps({"mode": "validate_only", "grants": [{"principal": "readers", "privileges": ["SELECT"]}]}),
        encoding="utf-8",
    )

    preview = governance_preview(load_contract_bundle(base))

    assert preview["target_table"] == "main.gold.gd_orders"
    assert preview["annotations_sql"]
    assert preview["access_sql"][0] == "GRANT SELECT ON TABLE `main`.`gold`.`gd_orders` TO `readers`"


def test_validate_governance_contract_detects_missing_columns():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        annotations={"columns": {"email": {"description": "Email"}}},
        access={
            "row_filters": [
                {"name": "by_region", "function": "main.security.fn_region", "columns": ["region"]}
            ],
            "column_masks": [
                {"column": "phone", "function": "main.security.mask_phone", "using_columns": ["phone"]}
            ],
        },
    )

    report = validate_governance_contract(
        "main.gold.orders",
        plan.annotations,
        plan.access,
        existing_columns=["email"],
    )

    assert report["status"] == "FAILED"
    assert {issue["object"] for issue in report["issues"]} == {"phone", "region"}


def test_validate_governance_contract_passes_existing_columns():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        annotations={"columns": {"email": {"description": "Email"}}},
        access={
            "row_filters": [
                {"name": "by_region", "function": "main.security.fn_region", "columns": ["region"]}
            ],
            "column_masks": [
                {"column": "email", "function": "main.security.mask_email", "using_columns": ["email"]}
            ],
        },
    )

    report = validate_governance_contract(
        "main.gold.orders",
        plan.annotations,
        plan.access,
        existing_columns=["email", "region"],
    )

    assert report["status"] == "SUCCESS"
    assert report["issues"] == []


def test_validate_governance_contract_flags_uc_only_features_on_unqualified_target():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        annotations={"table": {"tags": {"domain": "sales"}}},
        access={
            "row_filters": [
                {"name": "by_region", "function": "main.security.fn_region", "columns": ["region"]},
            ],
        },
    )

    report = validate_governance_contract(
        "orders",
        plan.annotations,
        plan.access,
        existing_columns=["region"],
    )

    assert report["status"] == "FAILED"
    assert {issue["scope"] for issue in report["issues"]} == {"annotations", "access"}


def test_access_drift_report_detects_missing_and_unmanaged_grants():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        access={
            "revoke_unmanaged": True,
            "grants": [
                {"principal": "readers", "privileges": ["SELECT"]},
                {"principal": "writers", "privileges": ["MODIFY"]},
            ],
        },
    )

    report = access_drift_report(
        "main.gold.orders",
        plan.access,
        current_grants={("readers", "SELECT"), ("legacy", "SELECT")},
    )

    assert report["status"] == "DRIFTED"
    assert report["missing_grants"] == [("writers", "MODIFY")]
    assert report["unmanaged_grants"] == [("legacy", "SELECT")]


def test_access_drift_fail_policy_marks_issues_as_fail():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        access={
            "access_policy": {"on_drift": "fail"},
            "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
        },
    )

    report = access_drift_report(
        "main.gold.orders",
        plan.access,
        current_grants=set(),
    )

    assert report["status"] == "DRIFTED"
    assert {issue["severity"] for issue in report["issues"]} == {"fail"}


def test_apply_access_contract_fails_before_apply_when_on_drift_fail(monkeypatch):
    executed = []
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        access={
            "access_policy": {"on_drift": "fail"},
            "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
        },
    )

    monkeypatch.setattr(
        governance_module,
        "access_drift_report",
        lambda target, contract: {
            "status": "DRIFTED",
            "target_table": target,
            "declared_grants": [("readers", "SELECT")],
            "current_grants": [],
            "missing_grants": [("readers", "SELECT")],
            "unmanaged_grants": [],
            "issues": [{"severity": "fail", "scope": "grant", "object": "readers:SELECT"}],
        },
    )
    monkeypatch.setattr(governance_module, "_execute_step", lambda sql: executed.append(sql))

    with pytest.raises(ValueError, match="Drift de access detectado"):
        apply_access_contract(
            {"access": "ops.ctrl_ingestion_access"},
            "run-1",
            "main.gold.orders",
            plan.access,
            lambda tables, run_id, target, entries: None,
        )
    assert executed == []


def test_apply_access_contract_blocks_revoke_without_force():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        access={
            "revoke_unmanaged": True,
            "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
        },
    )

    with pytest.raises(ValueError, match="--force-revoke"):
        apply_access_contract(
            {"access": "ops.ctrl_ingestion_access"},
            "run-1",
            "main.gold.orders",
            plan.access,
            lambda tables, run_id, target, entries: None,
        )


def test_apply_access_contract_revokes_unmanaged_grants(monkeypatch):
    executed = []
    logged = []
    plan = build_plan_from_kwargs(
        source="x",
        target_table="orders",
        access={
            "revoke_unmanaged": True,
            "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
        },
    )

    monkeypatch.setattr(
        governance_module,
        "access_drift_report",
        lambda target, contract: {
            "status": "DRIFTED",
            "target_table": target,
            "declared_grants": [("readers", "SELECT")],
            "current_grants": [("legacy", "SELECT")],
            "missing_grants": [("readers", "SELECT")],
            "unmanaged_grants": [("legacy", "SELECT")],
            "issues": [],
        },
    )
    monkeypatch.setattr(governance_module, "_execute_step", lambda sql: executed.append(sql))

    result = apply_access_contract(
        {"access": "ops.ctrl_ingestion_access"},
        "run-1",
        "main.gold.orders",
        plan.access,
        lambda tables, run_id, target, entries: logged.extend(entries),
        allow_revoke_unmanaged=True,
    )

    assert result["status"] == "SUCCESS"
    assert "GRANT SELECT ON TABLE `main`.`gold`.`orders` TO `readers`" in executed
    assert "REVOKE SELECT ON TABLE `main`.`gold`.`orders` FROM `legacy`" in executed
    assert any(entry["access_type"] == "revoke" and entry["previous_value"] == "GRANTED" for entry in logged)
