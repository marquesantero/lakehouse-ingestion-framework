"""Testes ponta-a-ponta dos 6 modos de escrita via ``ingest``."""
from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

from contractforge import IngestionHooks, ingest


def _common(target: str, layer: str = "silver"):
    return {
        "target_table": target,
        "catalog": "spark_catalog",
        "layer": layer,
        "ctrl_schema": "ops",
        "notebook_name": "test_modes",
    }


def test_scd0_append_creates_and_appends(spark, make_df, unique_name):
    table = f"{unique_name}_append"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    res1 = ingest(source=df1, mode="scd0_append", **_common(table, "bronze"))
    assert res1["status"] == "SUCCESS"
    assert res1["rows_written"] == 2

    df2 = make_df([(3, "c")], "id long, val string")
    res2 = ingest(source=df2, mode="scd0_append", **_common(table, "bronze"))
    assert res2["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.bronze.{table}")
    assert final.count() == 3


def test_scd0_overwrite_replaces_data(spark, make_df, unique_name):
    table = f"{unique_name}_overwrite"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    ingest(source=df1, mode="scd0_overwrite", **_common(table, "bronze"))

    df2 = make_df([(99, "z")], "id long, val string")
    ingest(source=df2, mode="scd0_overwrite", **_common(table, "bronze"))

    final = spark.table(f"spark_catalog.bronze.{table}")
    rows = sorted(r["id"] for r in final.collect())
    assert rows == [99]


def test_scd1_upsert_updates_existing_rows(spark, make_df, unique_name):
    table = f"{unique_name}_scd1"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    ingest(source=df1, mode="scd1_upsert", merge_keys="id", **_common(table))

    df2 = make_df([(2, "B_NEW"), (3, "c")], "id long, val string")
    ingest(source=df2, mode="scd1_upsert", merge_keys="id", **_common(table))

    final = spark.table(f"spark_catalog.silver.{table}")
    by_id = {r["id"]: r["val"] for r in final.collect()}
    assert by_id == {1: "a", 2: "B_NEW", 3: "c"}


def test_transform_deduplicate_runs_before_merge_safety(spark, make_df, unique_name):
    table = f"{unique_name}_transform_dedup"
    df = make_df(
        [
            (1, "old", "2026-05-01T10:00:00"),
            (1, "new", "2026-05-01T11:00:00"),
            (2, "only", "2026-05-01T09:00:00"),
        ],
        "id long, val string, updated_at string",
    )
    res = ingest(
        source=df,
        mode="scd1_upsert",
        merge_keys="id",
        transform={"deduplicate": {"keys": "id", "order_by": "updated_at DESC NULLS LAST"}},
        **_common(table),
    )
    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    by_id = {r["id"]: r["val"] for r in final.select("id", "val").collect()}
    assert by_id == {1: "new", 2: "only"}


def test_preset_silver_scd1_upsert_runs_end_to_end(spark, make_df, unique_name):
    table = f"{unique_name}_preset_scd1"
    df = make_df([(1, "a"), (2, "b")], "id long, val string")
    res = ingest(source=df, preset="silver_scd1_upsert", merge_keys="id", **_common(table))
    assert res["status"] == "SUCCESS"
    assert res["applied_presets"] == ["silver_scd1_upsert"]
    assert res["mode"] == "scd1_upsert"
    final = spark.table(f"spark_catalog.silver.{table}")
    assert final.count() == 2


def test_preset_gold_full_refresh_runs_end_to_end(spark, make_df, unique_name):
    table = f"{unique_name}_preset_gold"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    ingest(source=df1, preset="gold_full_refresh", **_common(table, "gold"))

    df2 = make_df([(3, "c")], "id long, val string")
    res = ingest(source=df2, preset="gold_full_refresh", **_common(table, "gold"))
    assert res["status"] == "SUCCESS"
    assert res["applied_presets"] == ["gold_full_refresh"]
    final = spark.table(f"spark_catalog.gold.{table}")
    assert sorted(r["id"] for r in final.collect()) == [3]


def test_column_mapping_renames_source_columns_before_write(spark, make_df, unique_name):
    table = f"{unique_name}_mapping"
    df = make_df([(1, "a"), (2, "b")], "src_id long, src_val string")
    res = ingest(
        source=df,
        mode="scd1_upsert",
        merge_keys="id",
        column_mapping={"src_id": "id", "src_val": "val"},
        **_common(table),
    )
    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    assert {"id", "val"}.issubset(set(final.columns))
    assert sorted((r["id"], r["val"]) for r in final.select("id", "val").collect()) == [(1, "a"), (2, "b")]


def test_shape_flattens_structs_and_extracts_nested_columns(spark, unique_name):
    table = f"{unique_name}_shape_flatten"
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField(
                "customer",
                StructType(
                    [
                        StructField("email", StringType(), True),
                        StructField("address", StructType([StructField("city", StringType(), True)]), True),
                    ]
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(
        [Row(id=1, customer=Row(email="a@example.com", address=Row(city="SP")))],
        schema,
    )

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "flatten": {"enabled": True, "include": ["customer"]},
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    assert {"customer_email", "customer_address_city", "id"}.issubset(set(final.columns))
    row = final.select("customer_email", "customer_address_city", "id").first()
    assert row["id"] == 1
    assert row["customer_email"] == "a@example.com"
    assert row["customer_address_city"] == "SP"


def test_shape_columns_project_sibling_nested_fields_when_alias_overwrites_parent(spark, unique_name):
    table = f"{unique_name}_shape_sibling_projection"
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField(
                "amount",
                StructType(
                    [
                        StructField("_VALUE", DoubleType(), True),
                        StructField("_currency", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame([Row(id="evt-1", amount=Row(_VALUE=10.5, _currency="USD"))], schema)

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "columns": {
                "id": "event_id",
                "amount._VALUE": {"alias": "amount", "cast": "DOUBLE"},
                "amount._currency": "currency",
            }
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    row = spark.table(f"spark_catalog.silver.{table}").select("event_id", "amount", "currency").first()
    assert row["event_id"] == "evt-1"
    assert row["amount"] == 10.5
    assert row["currency"] == "USD"


def test_json_like_string_is_not_parsed_when_shape_is_absent(spark, make_df, unique_name):
    table = f"{unique_name}_json_string_without_shape"
    df = make_df([(1, '{"customer":{"email":"a@example.com"}}')], "id long, payload string")

    res = ingest(source=df, mode="scd0_append", **_common(table, "silver"))

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    assert isinstance(final.schema["payload"].dataType, StringType)
    assert final.select("payload").first()["payload"] == '{"customer":{"email":"a@example.com"}}'


def test_shape_parses_json_string_before_arrays_and_columns(spark, make_df, unique_name):
    table = f"{unique_name}_shape_json_string"
    df = make_df(
        [
            (
                1,
                '{"customer":{"email":"a@example.com","address":{"city":"SP"}},"items":[{"sku":"A","qty":2},{"sku":"B","qty":1}],"tags":["vip","app"]}',
            )
        ],
        "order_id long, payload string",
    )

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "parse_json": [
                {
                    "column": "payload",
                    "schema": (
                        "STRUCT<customer: STRUCT<email: STRING, address: STRUCT<city: STRING>>, "
                        "items: ARRAY<STRUCT<sku: STRING, qty: BIGINT>>, tags: ARRAY<STRING>>"
                    ),
                }
            ],
            "arrays": [{"path": "payload.items", "mode": "explode_outer", "alias": "item"}],
            "columns": {
                "order_id": "order_id",
                "payload.customer.email": "customer_email",
                "payload.customer.address.city": "customer_city",
                "item.sku": "item_sku",
                "item.qty": "item_qty",
            },
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    assert {
        "customer_email",
        "item_sku",
        "item_qty",
        "customer_city",
    }.issubset(set(final.columns))
    assert "payload" not in final.columns
    rows = sorted(
        (r["order_id"], r["customer_email"], r["item_sku"], r["item_qty"], r["customer_city"])
        for r in final.select(
            "order_id",
            "customer_email",
            "item_sku",
            "item_qty",
            "customer_city",
        ).collect()
    )
    assert rows == [
        (1, "a@example.com", "A", 2, "SP"),
        (1, "a@example.com", "B", 1, "SP"),
    ]


def test_shape_parses_json_string_array_root(spark, make_df, unique_name):
    table = f"{unique_name}_shape_json_array_root"
    df = make_df([(1, '[{"sku":"A","qty":2},{"sku":"B","qty":1}]')], "order_id long, payload string")

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "parse_json": [{"column": "payload", "schema": "ARRAY<STRUCT<sku: STRING, qty: BIGINT>>"}],
            "arrays": [{"path": "payload", "mode": "explode_outer", "alias": "item"}],
            "columns": {"order_id": "order_id", "item.sku": "item_sku", "item.qty": "item_qty"},
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    rows = sorted(
        (r["order_id"], r["item_sku"], r["item_qty"])
        for r in final.select("order_id", "item_sku", "item_qty").collect()
    )
    assert rows == [(1, "A", 2), (1, "B", 1)]


def test_shape_parse_json_rejects_non_string_source(make_df, unique_name):
    table = f"{unique_name}_shape_json_non_string"
    df = make_df([(1, 10)], "id long, payload long")

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={"parse_json": [{"column": "payload", "schema": "STRUCT<a: STRING>"}]},
        **_common(table, "silver"),
    )

    assert res["status"] == "FAILED"
    assert "deve ser string" in (res["error_message"] or "")


def test_shape_explodes_arrays_of_structs_in_dependency_order(spark, unique_name):
    table = f"{unique_name}_shape_arrays"
    discount_schema = StructType([StructField("code", StringType(), True)])
    item_schema = StructType(
        [
            StructField("sku", StringType(), True),
            StructField("discounts", ArrayType(discount_schema), True),
        ]
    )
    order_schema = StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("items", ArrayType(item_schema), True),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(
                order_id=1,
                items=[
                    Row(sku="A", discounts=[Row(code="D1"), Row(code="D2")]),
                    Row(sku="B", discounts=[]),
                ],
            )
        ],
        order_schema,
    )

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "arrays": [
                {"path": "item.discounts", "mode": "explode_outer", "alias": "discount"},
                {"path": "items", "mode": "explode_outer", "alias": "item"},
            ],
            "columns": {"order_id": "order_id", "item.sku": "item_sku", "discount.code": "discount_code"},
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    rows = sorted(
        (r["order_id"], r["item_sku"], r["discount_code"])
        for r in final.select("order_id", "item_sku", "discount_code").collect()
    )
    assert rows == [(1, "A", "D1"), (1, "A", "D2"), (1, "B", None)]
    assert "item" not in final.columns
    assert "discount" not in final.columns


def test_shape_zips_parallel_arrays_before_explode(spark, unique_name):
    table = f"{unique_name}_shape_zip_arrays"
    hourly_schema = StructType(
        [
            StructField("time", ArrayType(StringType()), True),
            StructField("temperature_2m", ArrayType(LongType()), True),
            StructField("humidity", ArrayType(LongType()), True),
        ]
    )
    schema = StructType(
        [
            StructField("location_id", StringType(), False),
            StructField("hourly", hourly_schema, True),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(
                location_id="sp",
                hourly=Row(
                    time=["2026-05-14T00:00", "2026-05-14T01:00"],
                    temperature_2m=[21, 22],
                    humidity=[80, 78],
                ),
            )
        ],
        schema,
    )

    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "zip_arrays": [
                {
                    "alias": "hourly_rows",
                    "columns": {
                        "hourly.time": "time",
                        "hourly.temperature_2m": "temperature_2m",
                        "hourly.humidity": "humidity",
                    },
                }
            ],
            "arrays": [{"path": "hourly_rows", "mode": "explode_outer", "alias": "hour"}],
            "columns": {
                "location_id": "location_id",
                "hour.time": "forecast_hour",
                "hour.temperature_2m": {"alias": "temperature_2m", "cast": "DOUBLE"},
                "hour.humidity": "humidity",
                "humidity_ratio": {"expression": "humidity / 100.0", "alias": "humidity_ratio", "cast": "DOUBLE"},
            },
        },
        **_common(table, "silver"),
    )

    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.silver.{table}")
    rows = sorted(
        (r["location_id"], r["forecast_hour"], r["temperature_2m"], r["humidity"], r["humidity_ratio"])
        for r in final.select("location_id", "forecast_hour", "temperature_2m", "humidity", "humidity_ratio").collect()
    )
    assert rows == [
        ("sp", "2026-05-14T00:00", 21.0, 80, 0.8),
        ("sp", "2026-05-14T01:00", 22.0, 78, 0.78),
    ]
    assert "hourly_rows" not in final.columns
    assert "hour" not in final.columns


def test_shape_blocks_cardinality_change_on_bronze_by_default(make_df, unique_name):
    table = f"{unique_name}_shape_bronze_guard"
    df = make_df([(1, ["a", "b"])], "id long, items array<string>")
    res = ingest(
        source=df,
        mode="scd0_append",
        shape={"arrays": [{"path": "items", "mode": "explode_outer", "alias": "item"}]},
        **_common(table, "bronze"),
    )
    assert res["status"] == "FAILED"
    assert "bloqueado em bronze" in (res["error_message"] or "")


def test_shape_blocks_sibling_array_cartesian(spark, unique_name):
    table = f"{unique_name}_shape_cartesian"
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("items", ArrayType(StringType()), True),
            StructField("payments", ArrayType(StringType()), True),
        ]
    )
    df = spark.createDataFrame([Row(id=1, items=["a", "b"], payments=["pix", "card"])], schema)
    res = ingest(
        source=df,
        mode="scd0_append",
        shape={
            "arrays": [
                {"path": "items", "mode": "explode_outer", "alias": "item"},
                {"path": "payments", "mode": "explode_outer", "alias": "payment"},
            ]
        },
        **_common(table, "silver"),
    )
    assert res["status"] == "FAILED"
    assert "produto cartesiano" in (res["error_message"] or "")


def test_reserved_source_columns_are_recreated_not_carried(spark, make_df, unique_name):
    table = f"{unique_name}_reserved"
    df = make_df([(1, "2026-05-12")], "id long, ingestion_date string")
    res = ingest(source=df, mode="scd0_append", **_common(table, "bronze"))
    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.bronze.{table}")
    assert final.select("id").first()["id"] == 1
    assert final.schema["ingestion_date"].dataType.simpleString() == "date"


def test_reserved_source_column_can_be_preserved_with_column_mapping(spark, make_df, unique_name):
    table = f"{unique_name}_reserved_mapping"
    df = make_df([(1, "2026-05-12")], "id long, ingestion_date string")
    res = ingest(
        source=df,
        mode="scd0_append",
        column_mapping={"ingestion_date": "source_ingestion_date"},
        **_common(table, "bronze"),
    )
    assert res["status"] == "SUCCESS"
    final = spark.table(f"spark_catalog.bronze.{table}")
    row = final.select("id", "source_ingestion_date").first()
    assert row["id"] == 1
    assert row["source_ingestion_date"] == "2026-05-12"


def test_merge_keys_all_null_fail_before_merge(spark, make_df, unique_name):
    table = f"{unique_name}_nullkeys"
    df = make_df([(None, "a"), (None, "b")], "id long, val string")
    res = ingest(source=df, mode="scd1_upsert", merge_keys="id", **_common(table))
    assert res["status"] == "FAILED"
    assert "merge_keys totalmente nulas" in (res["error_message"] or "")


def test_duplicate_merge_keys_fail_before_merge_write(spark, make_df, unique_name):
    table = f"{unique_name}_dupkeys"
    df = make_df([(1, "a"), (1, "b"), (2, "c")], "id long, val string")

    res = ingest(source=df, mode="scd1_upsert", merge_keys="id", **_common(table))

    assert res["status"] == "FAILED"
    assert "linhas duplicadas" in (res["error_message"] or "")
    full = f"spark_catalog.silver.{table}"
    if spark.catalog.tableExists(full):
        assert spark.table(full).count() == 0


def test_delta_properties_are_applied_on_table_creation(spark, make_df, unique_name):
    table = f"{unique_name}_props"
    full = f"spark_catalog.bronze.{table}"
    df = make_df([(1, "a")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        delta_properties={"ingest.testProperty": "enabled"},
        **_common(table, "bronze"),
    )
    assert res["status"] == "SUCCESS"
    prop = spark.sql(f"SHOW TBLPROPERTIES {full} ('ingest.testProperty')").first()
    assert prop is not None
    assert prop["value"] == "enabled"


def test_hooks_can_transform_dataframe_and_observe_write(spark, make_df, unique_name):
    table = f"{unique_name}_hooks"
    observed = {}

    def after_prepare(df, plan):
        return df.withColumnRenamed("src_val", "val")

    def after_write(result, plan):
        observed["rows_written"] = result["rows_written"]

    hooks = IngestionHooks(after_prepare=after_prepare, after_write=after_write)
    df = make_df([(1, "a")], "id long, src_val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        hooks=hooks,
        **_common(table, "bronze"),
    )
    assert res["status"] == "SUCCESS"
    assert observed["rows_written"] == 1
    assert "val" in spark.table(f"spark_catalog.bronze.{table}").columns


def test_scd1_hash_diff_only_inserts_changes(spark, make_df, unique_name):
    table = f"{unique_name}_hash"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    ingest(source=df1, mode="scd1_hash_diff", hash_keys="id", **_common(table))
    base_count = spark.table(f"spark_catalog.silver.{table}").count()
    assert base_count == 2

    # Mesmos dados — nada deve ser inserido
    res2 = ingest(source=df1, mode="scd1_hash_diff", hash_keys="id", **_common(table))
    assert res2["rows_written"] == 0
    assert spark.table(f"spark_catalog.silver.{table}").count() == 2

    # Linha alterada — deve aparecer nova versão
    df3 = make_df([(2, "B_NEW")], "id long, val string")
    res3 = ingest(
        source=df3,
        mode="scd1_hash_diff",
        hash_keys="id",
        **_common(table),
    )
    assert res3["rows_written"] == 1


def test_scd2_historical_versions(spark, make_df, unique_name):
    table = f"{unique_name}_scd2"
    df1 = make_df([(1, "a"), (2, "b")], "id long, val string")
    ingest(
        source=df1,
        mode="scd2_historical",
        merge_keys="id",
        scd2_change_columns="val",
        **_common(table),
    )

    df2 = make_df([(2, "B_NEW")], "id long, val string")
    ingest(
        source=df2,
        mode="scd2_historical",
        merge_keys="id",
        scd2_change_columns="val",
        **_common(table),
    )

    final = spark.table(f"spark_catalog.silver.{table}")
    rows = final.select("id", "val", "is_current", "valid_to").collect()
    by_key = sorted([(r["id"], r["val"], r["is_current"]) for r in rows])
    assert (1, "a", True) in by_key
    assert (2, "b", False) in by_key
    assert (2, "B_NEW", True) in by_key


def test_snapshot_soft_delete_marks_missing(spark, make_df, unique_name):
    table = f"{unique_name}_snap"
    df1 = make_df([(1, "a"), (2, "b"), (3, "c")], "id long, val string")
    ingest(source=df1, mode="snapshot_soft_delete", merge_keys="id", **_common(table))

    df2 = make_df([(1, "a"), (3, "C_NEW")], "id long, val string")  # 2 some
    ingest(source=df2, mode="snapshot_soft_delete", merge_keys="id", **_common(table))

    final = spark.table(f"spark_catalog.silver.{table}")
    rows = {r["id"]: (r["is_active"], r["val"]) for r in final.collect()}
    assert rows[1] == (True, "a")
    assert rows[2][0] is False  # marcado inativo
    assert rows[3] == (True, "C_NEW")


def test_dry_run_does_not_write(spark, make_df, unique_name):
    table = f"{unique_name}_dry"
    df = make_df([(1, "a")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        dry_run=True,
        **_common(table, "bronze"),
    )
    assert res["status"] == "DRY_RUN"
    assert res["rows_written"] == 0
    # Tabela não deve ter sido criada com dados.
    full = f"spark_catalog.bronze.{table}"
    if spark.catalog.tableExists(full):
        assert spark.table(full).count() == 0


def test_dry_run_creates_no_ctrl_tables(spark, make_df, unique_name):
    """dry_run não deve criar schema/ctrl tables nem registrar a execução."""
    ctrl_schema = f"ops_dry_{unique_name}"
    table = f"{unique_name}_dry2"
    df = make_df([(1, "a")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        dry_run=True,
        target_table=table,
        catalog="spark_catalog",
        layer="bronze",
        ctrl_schema=ctrl_schema,
        notebook_name="test_dry_no_side_effects",
    )
    assert res["status"] == "DRY_RUN"
    schemas = {row["namespace"] for row in spark.sql("SHOW SCHEMAS IN spark_catalog").collect()}
    assert ctrl_schema not in schemas, "dry_run não deve criar schema de controle"


def test_snapshot_soft_delete_with_watermark_fails(spark, make_df, unique_name):
    """snapshot_soft_delete + watermark é inconsistente — deve falhar cedo."""
    table = f"{unique_name}_snap_wm"
    df = make_df([(1, "a", "2024-01-01")], "id long, val string, updated_at string")
    res = ingest(
        source=df,
        mode="snapshot_soft_delete",
        merge_keys="id",
        watermark_columns="updated_at",
        **_common(table),
    )
    assert res["status"] == "FAILED"
    assert "snapshot completo" in (res["error_message"] or "")


def test_snapshot_soft_delete_with_filter_fails(spark, make_df, unique_name):
    """snapshot_soft_delete + filter_expression também — snapshot precisa ser completo."""
    table = f"{unique_name}_snap_filter"
    df = make_df([(1, "a"), (2, "b")], "id long, val string")
    res = ingest(
        source=df,
        mode="snapshot_soft_delete",
        merge_keys="id",
        filter_expression="id > 0",
        **_common(table),
    )
    assert res["status"] == "FAILED"
    assert "snapshot completo" in (res["error_message"] or "")


def test_quarantine_escalates_on_unique_key_failure(spark, make_df, unique_name):
    """unique_key não é quarentenável — deve escalar para fail mesmo se on_quality_fail=quarantine."""
    table = f"{unique_name}_quar_unique"
    df = make_df([(1, "a"), (1, "b"), (2, "c")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        quality_rules={"unique_key": ["id"]},
        on_quality_fail="quarantine",
        **_common(table, "bronze"),
    )
    assert res["status"] == "FAILED"
    assert "Quality gates falharam" in (res["error_message"] or "")
    full = f"spark_catalog.bronze.{table}"
    if spark.catalog.tableExists(full):
        assert spark.table(full).count() == 0


def test_quarantine_escalates_on_min_rows_failure(spark, make_df, unique_name):
    """min_rows também não é quarentenável (propriedade do conjunto)."""
    table = f"{unique_name}_quar_minrows"
    df = make_df([(1, "a")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        quality_rules={"min_rows": 5},
        on_quality_fail="quarantine",
        **_common(table, "bronze"),
    )
    assert res["status"] == "FAILED"


def test_quality_fail_aborts_run(spark, make_df, unique_name):
    table = f"{unique_name}_qfail"
    df = make_df([(None, "a")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        quality_rules={"not_null": ["id"]},
        on_quality_fail="fail",
        **_common(table, "bronze"),
    )
    assert res["status"] == "FAILED"


def test_quality_quarantine_writes_valid_only(spark, make_df, unique_name):
    table = f"{unique_name}_qquar"
    df = make_df([(None, "a"), (1, "b"), (2, "c")], "id long, val string")
    res = ingest(
        source=df,
        mode="scd0_append",
        quality_rules={"not_null": ["id"]},
        on_quality_fail="quarantine",
        **_common(table, "bronze"),
    )
    assert res["status"] == "SUCCESS"
    assert res["rows_quarantined"] == 1
    final = spark.table(f"spark_catalog.bronze.{table}")
    assert final.count() == 2


def test_bronze_rejects_scd1_upsert(spark, make_df, unique_name):
    table = f"{unique_name}_bronze_scd1"
    df = make_df([(1,)], "id long")
    res = ingest(
        source=df,
        mode="scd1_upsert",
        merge_keys="id",
        **_common(table, "bronze"),
    )
    assert res["status"] == "FAILED"
    assert "Bronze" in (res["error_message"] or "")


def test_watermark_filters_already_seen(spark, make_df, unique_name):
    table = f"{unique_name}_wm"
    df1 = make_df(
        [(1, "2024-01-01"), (2, "2024-01-15")],
        "id long, updated_at string",
    )
    res1 = ingest(
        source=df1,
        mode="scd0_append",
        watermark_columns="updated_at",
        **_common(table, "bronze"),
    )
    assert res1["rows_written"] == 2

    df2 = make_df(
        [(3, "2024-01-10"), (4, "2024-01-20")],
        "id long, updated_at string",
    )
    res2 = ingest(
        source=df2,
        mode="scd0_append",
        watermark_columns="updated_at",
        **_common(table, "bronze"),
    )
    # Apenas id=4 entra (>2024-01-15)
    assert res2["rows_written"] == 1
