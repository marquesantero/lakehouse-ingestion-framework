"""Testes ponta-a-ponta dos 6 modos de escrita via ``ingest``."""
from __future__ import annotations

from lakehouse_ingestion import ingest


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
        dedup_order_expr="ingestion_date DESC NULLS LAST",
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
