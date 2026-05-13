from __future__ import annotations

import json

import pytest

import lakehouse_ingestion.governance as governance_module
import lakehouse_ingestion.ingestion as ingestion_module
from lakehouse_ingestion.cli import main
from lakehouse_ingestion.contract_schema import yaml_schema
from lakehouse_ingestion.plan import build_plan_from_kwargs
from lakehouse_ingestion.quality import QUALITY_RULE_REGISTRY, register_quality_rule
from lakehouse_ingestion.writers import register_write_mode


def test_yaml_schema_contains_new_contract_fields():
    schema = yaml_schema()
    props = schema["properties"]
    assert "column_mapping" in props
    assert "delta_properties" in props
    assert "retry_attempts" in props
    assert "annotations" in props
    assert "operations" in props
    assert "access" in props
    assert "preset" in props
    assert "shape" in props


def test_cli_validate_accepts_json_contract(tmp_path, capsys):
    contract = {
        "source": "raw_orders",
        "target_table": "b_orders",
        "mode": "scd0_append",
        "column_mapping": {"src_id": "id"},
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")

    assert main(["validate", str(path)]) == 0
    assert "OK" in capsys.readouterr().out


def test_cli_validate_can_expand_presets(tmp_path, capsys):
    contract = {
        "preset": ["silver_scd1_upsert", "quality_quarantine"],
        "source": "raw_orders",
        "target_table": "s_orders",
        "merge_keys": "id",
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")

    assert main(["validate", str(path), "--expand-presets"]) == 0
    output = capsys.readouterr().out
    assert '"applied_presets"' in output
    assert '"mode": "scd1_upsert"' in output
    assert "OK" in output


def test_cli_presets_list_and_show(capsys):
    assert main(["presets", "list", "--indent", "0"]) == 0
    output = capsys.readouterr().out
    assert "silver_scd1_upsert" in output

    assert main(["presets", "show", "gold_full_refresh", "--indent", "0"]) == 0
    output = capsys.readouterr().out
    assert '"name": "gold_full_refresh"' in output


def test_cli_governance_preview_accepts_split_contract(tmp_path, capsys):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.annotations.json").write_text(
        json.dumps({"table": {"description": "Gold orders"}}),
        encoding="utf-8",
    )

    assert main(["governance-preview", str(base), "--indent", "0"]) == 0
    output = capsys.readouterr().out
    assert "main.gold.gd_orders" in output
    assert "COMMENT ON TABLE" in output


def test_cli_governance_check_reports_missing_spark_schema(tmp_path, capsys):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )
    (tmp_path / "gd_orders.annotations.json").write_text(
        json.dumps({"columns": {"email": {"description": "Email"}}}),
        encoding="utf-8",
    )

    assert main(["governance-check", str(base), "--indent", "0"]) == 1
    output = capsys.readouterr().out
    assert "main.gold.gd_orders" in output
    assert "Nao foi possivel ler schema" in output


def test_cli_apply_access_uses_dedicated_command(tmp_path, monkeypatch, capsys):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )

    def fake_apply_access_bundle(path, *, force_revoke=False):
        return {"status": "SUCCESS", "path": path, "force_revoke": force_revoke}

    monkeypatch.setattr(ingestion_module, "apply_access_bundle", fake_apply_access_bundle)

    assert main(["apply-access", str(base), "--force-revoke"]) == 0
    output = capsys.readouterr().out
    assert '"force_revoke": true' in output


def test_cli_apply_annotations_uses_dedicated_command(tmp_path, monkeypatch, capsys):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )

    def fake_apply_annotations_bundle(path):
        return {"status": "SUCCESS", "path": path}

    monkeypatch.setattr(ingestion_module, "apply_annotations_bundle", fake_apply_annotations_bundle)

    assert main(["apply-annotations", str(base)]) == 0
    output = capsys.readouterr().out
    assert '"status": "SUCCESS"' in output


def test_cli_governance_apply_does_not_accept_force_revoke(tmp_path):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps({"source": "silver.orders", "target_table": "gd_orders", "layer": "gold"}),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        main(["governance-apply", str(base), "--force-revoke"])
    assert exc.value.code == 2


def test_cli_validate_access_returns_failed_on_fail_drift(tmp_path, monkeypatch, capsys):
    base = tmp_path / "gd_orders"
    (tmp_path / "gd_orders.ingestion.json").write_text(
        json.dumps(
            {
                "source": "silver.orders",
                "target_table": "gd_orders",
                "layer": "gold",
                "access": {
                    "access_policy": {"on_drift": "fail"},
                    "grants": [{"principal": "readers", "privileges": ["SELECT"]}],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        governance_module,
        "access_drift_report",
        lambda target, access: {
            "status": "DRIFTED",
            "target_table": target,
            "missing_grants": [("readers", "SELECT")],
            "unmanaged_grants": [],
            "issues": [{"severity": "fail", "scope": "grant", "object": "readers:SELECT"}],
        },
    )
    monkeypatch.setattr(
        governance_module,
        "validate_governance_contract",
        lambda target, annotations, access: {
            "status": "SUCCESS",
            "target_table": target,
            "references": {},
            "issues": [],
        },
    )

    assert main(["validate-access", str(base), "--indent", "0"]) == 1
    output = capsys.readouterr().out
    assert '"status": "FAILED"' in output


def test_write_mode_registry_extends_plan_validation():
    mode = "custom_unit_test_mode"

    def handler(plan, df, target, effective_rows):
        return effective_rows

    register_write_mode(mode, handler)
    plan = build_plan_from_kwargs(source="x", target_table="t", mode=mode)
    assert plan.mode == mode


def test_quality_rule_registry_registers_custom_evaluator():
    rule_type = "custom_unit_test_quality"

    def evaluator(df, rule_name, config):
        return {"failed_count": 0}

    register_quality_rule(rule_type, evaluator)
    assert QUALITY_RULE_REGISTRY[rule_type] is evaluator
