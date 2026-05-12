from __future__ import annotations

import json

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
