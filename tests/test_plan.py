"""Testes puros: build_plan_from_kwargs, validações de modo, normalização."""
from __future__ import annotations

import pytest

from contractforge import (
    IngestionPlan,
    DeduplicateConfig,
    QualityExpression,
    QualityRules,
    ShapeConfig,
    SourceSpec,
    TransformConfig,
    apply_preset,
    get_preset,
    ingest,
    list_presets,
    register_preset,
)
from contractforge.plan import (
    build_plan_from_kwargs,
    normalize_quality_rules,
    target_full_table_name,
    target_schema_name,
    validate_plan_shape,
    validate_write_mode,
)
from contractforge.hooks import IngestionHooks
from contractforge.ingestion import _validate_static_plan_options


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
    qr = normalize_quality_rules({"not_null": "a", "min_rows": 5})
    assert isinstance(qr, QualityRules)
    assert qr.not_null == ["a"]
    assert qr.min_rows == 5


def test_normalize_quality_rules_rejects_unknown_fields():
    with pytest.raises(ValueError, match="campos não reconhecidos"):
        normalize_quality_rules({"not_null": ["a"], "custom_rule": True})


def test_normalize_quality_rules_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="min_rows"):
        normalize_quality_rules({"min_rows": 0})
    with pytest.raises(ValueError, match="max_null_ratio.email"):
        normalize_quality_rules({"max_null_ratio": {"email": 1.5}})
    with pytest.raises(ValueError, match="max_null_ratio deve ser um objeto"):
        normalize_quality_rules({"max_null_ratio": 0})


def test_normalize_quality_rules_normalizes_accepted_values_string():
    qr = normalize_quality_rules({"accepted_values": {"status": "open|closed"}})
    assert qr.accepted_values == {"status": ["open", "closed"]}


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
    assert target_schema_name(plan) == "bronze"
    assert target_full_table_name(plan) == "c1.bronze.b_orders"


def test_build_plan_accepts_target_schema_override():
    plan = build_plan_from_kwargs(
        source="raw_orders",
        target_table="orders",
        catalog="main",
        layer="silver",
        target_schema="crm_curated",
    )
    assert plan.layer == "silver"
    assert plan.target_schema == "crm_curated"
    assert target_schema_name(plan) == "crm_curated"
    assert target_full_table_name(plan) == "main.crm_curated.orders"


def test_build_plan_accepts_custom_logical_layer_with_physical_schema():
    plan = build_plan_from_kwargs(
        source="raw_orders",
        target_table="orders",
        catalog="main",
        layer="stage",
        target_schema="staging_area",
    )
    assert plan.layer == "stage"
    assert target_schema_name(plan) == "staging_area"
    assert target_full_table_name(plan) == "main.staging_area.orders"


def test_build_plan_accepts_target_block_alias():
    plan = build_plan_from_kwargs(
        source="raw_orders",
        target={"catalog": "main", "schema": "custom_schema", "table": "orders"},
        layer="gold",
    )
    assert plan.catalog == "main"
    assert plan.target_schema == "custom_schema"
    assert plan.target_table == "orders"
    assert target_full_table_name(plan) == "main.custom_schema.orders"


def test_build_plan_rejects_conflicting_target_block():
    with pytest.raises(ValueError, match="conflita"):
        build_plan_from_kwargs(
            source="raw_orders",
            target_table="orders",
            target={"table": "other_orders"},
        )


def test_builtin_presets_cover_common_ingestion_modes():
    assert len(list_presets()) >= 15
    for name in [
        "bronze_autoloader_append",
        "silver_scd1_upsert",
        "silver_hash_diff_append",
        "silver_snapshot_soft_delete",
        "silver_scd2_historical",
        "gold_full_refresh",
        "gold_replace_partitions",
    ]:
        assert name in list_presets()


def test_apply_preset_merges_defaults_and_contract_wins():
    expanded = apply_preset(
        {
            "preset": ["silver_scd1_upsert", "quality_quarantine", "delta_cdf_enabled"],
            "source": "raw.orders",
            "target_table": "s_orders",
            "merge_keys": "id",
            "schema_policy": "strict",
        }
    )
    assert expanded["applied_presets"] == [
        "silver_scd1_upsert",
        "quality_quarantine",
        "delta_cdf_enabled",
    ]
    assert expanded["mode"] == "scd1_upsert"
    assert expanded["layer"] == "silver"
    assert expanded["schema_policy"] == "strict"
    assert expanded["on_quality_fail"] == "quarantine"
    assert expanded["delta_properties"] == {"delta.enableChangeDataFeed": "true"}


def test_build_plan_accepts_preset_and_records_applied_presets():
    plan = build_plan_from_kwargs(
        preset=["silver_scd1_upsert", "quality_quarantine"],
        source="raw.orders",
        target_table="s_orders",
        merge_keys="id",
    )
    assert plan.mode == "scd1_upsert"
    assert plan.layer == "silver"
    assert plan.merge_keys == ["id"]
    assert plan.on_quality_fail == "quarantine"
    assert plan.applied_presets == ["silver_scd1_upsert", "quality_quarantine"]


def test_preset_required_fields_are_validated_before_plan_build():
    with pytest.raises(ValueError, match="silver_scd1_upsert:merge_keys"):
        build_plan_from_kwargs(
            preset="silver_scd1_upsert",
            source="raw.orders",
            target_table="s_orders",
        )


def test_preset_rejects_multiple_ingestion_or_runtime_presets():
    with pytest.raises(ValueError, match="tipo ingestion"):
        build_plan_from_kwargs(
            preset=["silver_scd1_upsert", "gold_full_refresh"],
            source="raw.orders",
            target_table="orders",
            merge_keys="id",
        )
    with pytest.raises(ValueError, match="tipo runtime"):
        build_plan_from_kwargs(
            preset=["runtime_databricks_serverless", "runtime_spark_delta_local"],
            source="raw.orders",
            target_table="orders",
        )


def test_register_preset_supports_custom_extensions():
    name = "unit_company_silver"
    register_preset(
        name,
        {
            "layer": "silver",
            "mode": "scd1_upsert",
            "merge_strategy": "delta",
            "_preset": {
                "kind": "ingestion",
                "category": "silver",
                "required_fields": ["merge_keys"],
            },
        },
        override=True,
    )
    plan = build_plan_from_kwargs(
        preset=name,
        source="raw.orders",
        target_table="s_orders",
        merge_keys="id",
    )
    assert plan.applied_presets == [name]
    assert get_preset(name)["mode"] == "scd1_upsert"
    with pytest.raises(ValueError, match="já registrado"):
        register_preset(name, {"mode": "scd0_append"})


def test_build_plan_normalizes_pipe_separated_lists():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        merge_keys="id|tenant_id",
        watermark_columns="updated_at",
    )
    assert plan.merge_keys == ["id", "tenant_id"]
    assert plan.watermark_columns == ["updated_at"]


def test_build_plan_accepts_mapping_delta_properties_and_retry_options():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        column_mapping={"src_id": "id", "src_name": "name"},
        delta_properties={"delta.enableChangeDataFeed": True},
        retry_attempts="5",
        retry_backoff_seconds="0",
    )
    assert plan.column_mapping == {"src_id": "id", "src_name": "name"}
    assert plan.delta_properties == {"delta.enableChangeDataFeed": "true"}
    assert plan.retry_attempts == 5
    assert plan.retry_backoff_seconds == 0


def test_build_plan_accepts_shape_contract():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        shape={
            "parse_json": [
                {
                    "column": "payload",
                    "schema": "STRUCT<customer: STRUCT<email: STRING>, items: ARRAY<STRUCT<sku: STRING>>>",
                    "alias": "payload_json",
                    "drop_source": True,
                }
            ],
            "flatten": {"enabled": True, "separator": "_", "max_depth": 4},
            "arrays": [
                {"path": "payload_json.items", "mode": "explode_outer", "alias": "item"},
                {"path": "item.discounts", "mode": "to_json", "alias": "discounts_json"},
            ],
            "zip_arrays": [
                {
                    "alias": "hourly_rows",
                    "columns": {
                        "payload_json.hourly.time": "time",
                        "payload_json.hourly.temperature_2m": "temperature_2m",
                    },
                }
            ],
            "columns": {
                "payload_json.customer.email": {"alias": "customer_email"},
                "item.sku": {"alias": "item_sku", "cast": "STRING"},
                "item_qty_numeric": {"expression": "CAST(item.qty AS BIGINT)", "alias": "item_qty"},
            },
            "allow_cardinality_change_on_bronze": True,
        },
    )
    assert isinstance(plan.shape, ShapeConfig)
    assert plan.shape.parse_json[0].column == "payload"
    assert plan.shape.parse_json[0].alias == "payload_json"
    assert plan.shape.parse_json[0].drop_source is True
    assert plan.shape.flatten.enabled is True
    assert plan.shape.arrays[0].path == "payload_json.items"
    assert plan.shape.arrays[0].mode == "explode_outer"
    assert plan.shape.zip_arrays[0].alias == "hourly_rows"
    assert plan.shape.zip_arrays[0].columns["payload_json.hourly.time"] == "time"
    assert plan.shape.columns["payload_json.customer.email"].alias == "customer_email"
    assert plan.shape.columns["item.sku"].cast == "STRING"
    assert plan.shape.columns["item_qty_numeric"].expression == "CAST(item.qty AS BIGINT)"
    assert isinstance(plan.transform, TransformConfig)
    assert plan.transform.shape == plan.shape


def test_build_plan_accepts_transform_shape_contract():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        transform={
            "shape": {
                "parse_json": [
                    {
                        "column": "payload",
                        "schema": "STRUCT<id: STRING>",
                        "alias": "payload_json",
                    }
                ],
                "columns": {"payload_json.id": "event_id"},
            }
        },
    )
    assert isinstance(plan.shape, ShapeConfig)
    assert plan.transform.shape == plan.shape
    assert plan.shape.parse_json[0].alias == "payload_json"
    assert plan.shape.columns["payload_json.id"].alias == "event_id"


def test_build_plan_accepts_transform_deduplicate_contract():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        transform={
            "deduplicate": {
                "keys": "id|tenant_id",
                "order_by": "updated_at DESC NULLS LAST, sequence DESC",
            }
        },
    )
    assert isinstance(plan.transform.deduplicate, DeduplicateConfig)
    assert plan.transform.deduplicate.keys == ["id", "tenant_id"]
    assert plan.transform.deduplicate.order_by == "updated_at DESC NULLS LAST, sequence DESC"
    assert plan.dedup_order_expr == "updated_at DESC NULLS LAST, sequence DESC"


def test_build_plan_rejects_conflicting_transform_aliases():
    with pytest.raises(ValueError, match="shape e transform.shape conflitam"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={"columns": {"a": "a"}},
            transform={"shape": {"columns": {"b": "b"}}},
        )
    with pytest.raises(ValueError, match="dedup_order_expr e transform.deduplicate.order_by conflitam"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            dedup_order_expr="updated_at DESC",
            transform={"deduplicate": {"keys": "id", "order_by": "sequence DESC"}},
        )
    with pytest.raises(ValueError, match="transform.deduplicate.keys"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            transform={"deduplicate": {"keys": [], "order_by": "updated_at DESC"}},
        )
    with pytest.raises(ValueError, match="transform.deduplicate.order_by"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            transform={"deduplicate": {"keys": "id", "order_by": " "}},
        )


def test_build_plan_rejects_invalid_shape_contract():
    with pytest.raises(ValueError, match="shape.arrays deve ser uma lista"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"arrays": {"path": "items"}})
    with pytest.raises(ValueError, match="mode='bad'"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"arrays": [{"path": "items", "mode": "bad"}]})
    with pytest.raises(ValueError, match="sem ponto"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"columns": {"a.b": {"alias": "x.y"}}})
    with pytest.raises(ValueError, match="shape.parse_json deve ser uma lista"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"parse_json": {"column": "payload"}})
    with pytest.raises(ValueError, match="schema não pode ser vazio"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"parse_json": [{"column": "payload"}]})
    with pytest.raises(ValueError, match="alias é obrigatório"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={"parse_json": [{"column": "envelope.payload", "schema": "STRUCT<a: STRING>"}]},
        )
    with pytest.raises(ValueError, match="drop_source não é suportado"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={
                "parse_json": [
                    {
                        "column": "envelope.payload",
                        "schema": "STRUCT<a: STRING>",
                        "alias": "payload_json",
                        "drop_source": True,
                    }
                ]
            },
        )
    with pytest.raises(ValueError, match="shape.zip_arrays deve ser uma lista"):
        build_plan_from_kwargs(source="x", target_table="t", shape={"zip_arrays": {"alias": "rows"}})
    with pytest.raises(ValueError, match="pelo menos dois arrays"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={"zip_arrays": [{"alias": "rows", "columns": {"a": "a"}}]},
        )
    with pytest.raises(ValueError, match="campo de saída duplicado"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={"zip_arrays": [{"alias": "rows", "columns": {"a": "value", "b": "value"}}]},
        )
    with pytest.raises(ValueError, match="não pode ser vazio"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            shape={"columns": {"a": {"alias": "x", "cast": ""}}},
        )


def test_build_plan_rejects_invalid_mapping_and_retry_options():
    with pytest.raises(ValueError, match="mesmo destino"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            column_mapping={"a": "id", "b": "id"},
        )
    with pytest.raises(ValueError, match="colunas técnicas reservadas"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            column_mapping={"src_run": "__run_id"},
        )
    with pytest.raises(ValueError, match="retry_attempts"):
        build_plan_from_kwargs(source="x", target_table="t", retry_attempts=0)
    with pytest.raises(ValueError, match="retry_backoff_seconds"):
        build_plan_from_kwargs(source="x", target_table="t", retry_backoff_seconds=-1)


def test_build_plan_accepts_programmatic_hooks():
    hooks = IngestionHooks(before_read=lambda plan: None)
    plan = build_plan_from_kwargs(source="x", target_table="t", hooks=hooks)
    assert plan.hooks is hooks


def test_build_plan_normalizes_source_spec_from_dict():
    plan = build_plan_from_kwargs(
        source={
            "type": "autoloader",
            "path": "/Volumes/main/raw/orders",
            "format": "json",
            "schema_location": "/Volumes/main/ops/schemas/orders",
            "checkpoint_location": "/Volumes/main/ops/checkpoints/orders",
            "trigger": "available_now",
            "options": {"cloudFiles.inferColumnTypes": "true"},
            "max_files_per_trigger": "10",
        },
        target_table="b_orders",
    )
    assert isinstance(plan.source, SourceSpec)
    assert plan.source.type == "autoloader"
    assert plan.source.format == "json"
    assert plan.source.options == {"cloudFiles.inferColumnTypes": "true"}
    assert plan.source.max_files_per_trigger == 10


@pytest.mark.parametrize(
    "source, match",
    [
        ({"type": "autoloader"}, "source.path"),
        (
            {"type": "autoloader", "path": "/x", "checkpoint_location": "/c"},
            "schema_location",
        ),
        (
            {"type": "autoloader", "path": "/x", "schema_location": "/s"},
            "checkpoint_location",
        ),
        (
            {
                "type": "files",
                "path": "/x",
                "schema_location": "/s",
                "checkpoint_location": "/c",
            },
            "source.type",
        ),
    ],
)
def test_build_plan_rejects_invalid_source_spec(source, match):
    with pytest.raises(ValueError, match=match):
        build_plan_from_kwargs(source=source, target_table="b_orders")


def test_source_spec_rejects_snapshot_soft_delete():
    with pytest.raises(ValueError, match="snapshot_soft_delete"):
        build_plan_from_kwargs(
            source={
                "type": "autoloader",
                "path": "/x",
                "schema_location": "/s",
                "checkpoint_location": "/c",
            },
            target_table="snapshot",
            mode="snapshot_soft_delete",
            merge_keys="id",
        )


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


def test_build_plan_quality_rules_custom_dict():
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        quality_rules={
            "custom": {
                "freshness": {
                    "type": "freshness",
                    "column": "updated_at",
                    "max_age_hours": 24,
                    "severity": "warn",
                }
            }
        },
    )
    assert plan.quality_rules.custom["freshness"]["type"] == "freshness"
    assert plan.quality_rules.custom["freshness"]["severity"] == "warn"


def test_build_plan_rejects_invalid_custom_quality_rule():
    with pytest.raises(ValueError, match="type"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            quality_rules={"custom": {"freshness": {"column": "updated_at"}}},
        )


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
        ({"layer": "bad layer"}, "layer"),
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


def test_build_plan_requires_idempotency_key_for_non_default_policy():
    with pytest.raises(ValueError, match="idempotency_key"):
        build_plan_from_kwargs(source="x", target_table="t", idempotency_policy="skip_if_success")


def test_build_plan_rejects_strict_schema_with_type_widening():
    with pytest.raises(ValueError, match="allow_type_widening"):
        build_plan_from_kwargs(
            source="x",
            target_table="t",
            schema_policy="strict",
            allow_type_widening=True,
        )


def test_validate_plan_shape_rejects_empty_contract_fields():
    plan = IngestionPlan(source="x", target_table=" ")
    with pytest.raises(ValueError, match="target_table"):
        validate_plan_shape(plan)


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
