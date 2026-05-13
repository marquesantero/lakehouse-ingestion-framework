from __future__ import annotations

import json

import pytest

from lakehouse_ingestion.contract_bundle import governance_preview, load_contract_bundle
from lakehouse_ingestion.governance import (
    AccessContract,
    AnnotationsContract,
    OperationsContract,
    access_sql_preview,
    annotation_sql_preview,
    validate_governance_contract,
)
from lakehouse_ingestion.plan import build_plan_from_kwargs


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
        json.dumps({"table": {"description": "Gold orders"}}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.operations.json").write_text(
        json.dumps({"criticality": "critical", "owners": ["data-platform"]}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.access.json").write_text(
        json.dumps({"mode": "validate_only", "grants": [{"principal": "readers", "privileges": ["SELECT"]}]}),
        encoding="utf-8",
    )

    bundle = load_contract_bundle(base)

    assert bundle.ingestion.target_table == "gd_orders"
    assert bundle.annotations.table.description == "Gold orders"
    assert bundle.operations.criticality == "critical"
    assert bundle.access.mode == "validate_only"
    assert bundle.metadata["ingestion"]["contract_version"] == "1.0.0"
    assert "ingestion" in bundle.paths


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
