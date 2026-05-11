"""Testes puros: build_plan_from_kwargs, validações de modo, normalização."""
from __future__ import annotations

import pytest

from lakehouse_ingestion import IngestionPlan, QualityExpression, QualityRules, ingest
from lakehouse_ingestion.plan import (
    build_plan_from_kwargs,
    normalize_quality_rules,
    validate_write_mode,
)
from lakehouse_ingestion.ingestion import _validate_static_plan_options


def test_validate_write_mode_accepts_valid():
    assert validate_write_mode("scd0_append") == "scd0_append"
    assert validate_write_mode("scd2_historical") == "scd2_historical"


def test_validate_write_mode_default_when_missing():
    assert validate_write_mode(None) == "scd0_append"
    assert validate_write_mode("") == "scd0_append"


def test_validate_write_mode_rejects_unknown():
    with pytest.raises(ValueError, match="Modo de escrita não suportado"):
        validate_write_mode("scd9_inventado")


def test_normalize_quality_rules_passthrough():
    qr = QualityRules(not_null=["a"])
    assert normalize_quality_rules(qr) is qr


def test_normalize_quality_rules_from_dict():
    qr = normalize_quality_rules({"not_null": ["a"], "min_rows": 5})
    assert isinstance(qr, QualityRules)
    assert qr.not_null == ["a"]
    assert qr.min_rows == 5


def test_normalize_quality_rules_none():
    assert normalize_quality_rules(None) is None


def test_build_plan_basic():
    plan = build_plan_from_kwargs(
        source="raw_orders",
        target_table="b_orders",
        catalog="c1",
        layer="bronze",
        mode="scd0_append",
    )
    assert isinstance(plan, IngestionPlan)
    assert plan.target_table == "b_orders"
    assert plan.mode == "scd0_append"
    assert plan.merge_keys == []


def test_build_plan_normalizes_pipe_separated_lists():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        merge_keys="id|tenant_id",
        watermark_columns="updated_at",
    )
    assert plan.merge_keys == ["id", "tenant_id"]
    assert plan.watermark_columns == ["updated_at"]


def test_build_plan_rejects_unknown_kwargs():
    with pytest.raises(ValueError, match="não reconhecidos"):
        build_plan_from_kwargs(source="x", target_table="t", invalid_param=True)


def test_build_plan_quality_rules_dict():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        quality_rules={"not_null": ["id"], "min_rows": 1},
    )
    assert isinstance(plan.quality_rules, QualityRules)
    assert plan.quality_rules.not_null == ["id"]


def test_build_plan_quality_rules_expressions_dict():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        quality_rules={
            "expressions": [
                {"name": "positive_amount", "expression": "amount > 0"},
                {
                    "name": "valid_period",
                    "expression": "end_date >= start_date",
                    "severity": "abort",
                    "message": "Período inválido.",
                },
            ]
        },
    )
    assert isinstance(plan.quality_rules.expressions[0], QualityExpression)
    assert plan.quality_rules.expressions[0].severity == "quarantine"
    assert plan.quality_rules.expressions[1].severity == "abort"
    assert plan.quality_rules.expressions[1].message == "Período inválido."


def test_build_plan_rejects_invalid_quality_expression_severity():
    with pytest.raises(ValueError, match="quality_rules.expressions.severity"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            quality_rules={"expressions": [{"name": "x", "expression": "id > 0", "severity": "block"}]},
        )


def test_ingest_rejects_unknown_kwargs(monkeypatch):
    """A função pública não deve aceitar parâmetros desconhecidos."""
    with pytest.raises(ValueError):
        ingest(source="x", target_table="t", typo_param=1)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"layer": "platinum"}, "layer"),
        ({"merge_strategy": "delta_full"}, "merge_strategy"),
        ({"schema_policy": "loose"}, "schema_policy"),
        ({"on_quality_fail": "ignore"}, "on_quality_fail"),
        ({"explain_format": "json"}, "explain_format"),
    ],
)
def test_build_plan_rejects_invalid_enums(kwargs, match):
    """Typos de enum devem virar ValueError, não silently passar."""
    with pytest.raises(ValueError, match=match):
        build_plan_from_kwargs(source="x", target_table="t", **kwargs)


def test_build_plan_accepts_all_valid_enums():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        catalog="c1",
        layer="silver",
        mode="scd1_upsert",
        merge_keys="id",
        merge_strategy="delta_by_partition",
        schema_policy="strict",
        on_quality_fail="warn",
        explain_format="extended",
    )
    assert plan.layer == "silver"
    assert plan.merge_strategy == "delta_by_partition"
    assert plan.schema_policy == "strict"
    assert plan.on_quality_fail == "warn"
    assert plan.explain_format == "extended"


def test_build_plan_accepts_idempotency_options():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        idempotency_key="job-42:batch-2026-05-11",
        idempotency_policy="skip_if_success",
    )
    assert plan.idempotency_key == "job-42:batch-2026-05-11"
    assert plan.idempotency_policy == "skip_if_success"


def test_build_plan_accepts_explicit_idempotency_policy():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        idempotency_key="job-42:batch-2026-05-11",
        idempotency_policy="fail_if_success",
    )
    assert plan.idempotency_policy == "fail_if_success"


def test_build_plan_accepts_contract_metadata_and_schema_widening():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        description="Customer dimension",
        owner="data-platform",
        domain="sales",
        tags="customer|gold",
        sla="daily 08:00",
        runtime_parameters={"env": "dev"},
        allow_type_widening=True,
    )
    assert plan.description == "Customer dimension"
    assert plan.owner == "data-platform"
    assert plan.domain == "sales"
    assert plan.tags == ["customer", "gold"]
    assert plan.sla == "daily 08:00"
    assert plan.runtime_parameters == {"env": "dev"}
    assert plan.allow_type_widening is True


def test_build_plan_rejects_invalid_idempotency_policy():
    with pytest.raises(ValueError, match="idempotency_policy"):
        build_plan_from_kwargs(source="x", target_table="t", idempotency_policy="skip_maybe")


def test_build_plan_accepts_replace_partitions_source_complete():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        mode="scd1_upsert",
        merge_keys="id",
        merge_strategy="replace_partitions",
        merge_partition_column="dt",
        replace_partitions_source_complete=True,
    )
    assert plan.replace_partitions_source_complete is True


def test_replace_partitions_requires_explicit_complete_source_confirmation():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        layer="silver",
        mode="scd1_upsert",
        merge_keys="id",
        merge_strategy="replace_partitions",
        merge_partition_column="dt",
    )
    with pytest.raises(ValueError, match="replace_partitions_source_complete=True"):
        _validate_static_plan_options(plan)


def test_replace_partitions_rejects_mismatched_partition_columns():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        layer="silver",
        mode="scd1_upsert",
        merge_keys="id",
        merge_strategy="replace_partitions",
        partition_column="ingestion_date",
        merge_partition_column="dt",
        replace_partitions_source_complete=True,
    )
    with pytest.raises(ValueError, match="partition_column igual"):
        _validate_static_plan_options(plan)


def test_replace_partitions_static_validation_accepts_complete_partition_snapshot():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        layer="silver",
        mode="scd1_upsert",
        merge_keys="id",
        merge_strategy="replace_partitions",
        merge_partition_column="dt",
        replace_partitions_source_complete=True,
    )
    _validate_static_plan_options(plan)
