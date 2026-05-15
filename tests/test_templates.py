from __future__ import annotations

import json

from contractforge.cli import main
from contractforge.plan import build_plan_from_kwargs
from contractforge.templates import (
    contract_template_details,
    contract_template_files,
    list_contract_templates,
)


def test_builtin_templates_cover_core_scenarios():
    names = set(list_contract_templates())

    assert {
        "bronze_rest_api_incremental",
        "bronze_autoloader_json",
        "silver_jdbc_scd1_upsert",
        "silver_snapshot_soft_delete",
        "silver_scd2_history",
        "gold_full_refresh_kpi",
    } <= names


def test_builtin_template_ingestion_contracts_are_valid():
    for name in list_contract_templates():
        files = contract_template_files(name)
        plan = build_plan_from_kwargs(**files["ingestion"])
        assert plan.target_table
        assert plan.applied_presets


def test_template_details_are_metadata_only():
    details = contract_template_details("silver_jdbc_scd1_upsert")

    assert details["name"] == "silver_jdbc_scd1_upsert"
    assert details["category"] == "silver"
    assert details["files"] == ["ingestion", "annotations", "operations", "access"]
    assert details["target"]["schema"] == "sales_curated"


def test_cli_templates_list_and_show(capsys):
    assert main(["templates", "list", "--indent", "0"]) == 0
    output = capsys.readouterr().out
    assert "bronze_rest_api_incremental" in output

    assert main(["templates", "show", "gold_full_refresh_kpi", "--metadata-only", "--indent", "0"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["name"] == "gold_full_refresh_kpi"
    assert "ingestion" in payload["files"]


def test_cli_templates_write_generates_valid_bundle(tmp_path, capsys):
    base = tmp_path / "contracts" / "silver" / "s_orders"

    assert (
        main(
            [
                "templates",
                "write",
                "silver_jdbc_scd1_upsert",
                "--output",
                str(base),
                "--indent",
                "0",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "SUCCESS"
    assert (tmp_path / "contracts" / "silver" / "s_orders.ingestion.yaml").exists()
    assert (tmp_path / "contracts" / "silver" / "s_orders.access.yaml").exists()

    assert main(["validate-bundle", str(base)]) == 0
    assert "OK" in capsys.readouterr().out


def test_cli_templates_write_refuses_overwrite_without_force(tmp_path, capsys):
    base = tmp_path / "contracts" / "bronze" / "b_orders"

    assert main(["templates", "write", "bronze_rest_api_incremental", "--output", str(base)]) == 0
    capsys.readouterr()
    assert main(["templates", "write", "bronze_rest_api_incremental", "--output", str(base)]) == 1
    assert "ja existe" in capsys.readouterr().err
