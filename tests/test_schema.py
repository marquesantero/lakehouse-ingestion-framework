"""Testes de schema policy, hash, dedup e custom keys."""
from __future__ import annotations

import pytest

from contractforge.schema import (
    add_row_hash,
    build_custom_keys,
    deduplicate_by_order,
    fix_encoding,
    hash_columns,
    is_type_widening,
    sync_delta_schema,
    table_exists,
    validate_schema_policy,
)


def test_is_type_widening_accepts_safe_widening():
    assert is_type_widening("bigint", "int") is True
    assert is_type_widening("double", "float") is True
    assert is_type_widening("decimal(12,2)", "decimal(10,2)") is True
    assert is_type_widening("timestamp", "date") is True


def test_is_type_widening_rejects_narrowing_or_unsafe_changes():
    assert is_type_widening("int", "bigint") is False
    assert is_type_widening("decimal(10,2)", "decimal(12,2)") is False
    assert is_type_widening("string", "int") is False


def test_hash_columns_excludes_control(make_df):
    df = make_df([(1, "x", "ev")], "id long, name string, source_system string")
    cols = hash_columns(df)
    assert "source_system" not in cols
    assert set(cols) == {"id", "name"}


def test_add_row_hash_is_deterministic(make_df):
    df = make_df(
        [(1, "a"), (1, "a"), (1, "b")],
        "id long, name string",
    )
    df_h = add_row_hash(df).collect()
    assert df_h[0]["row_hash"] == df_h[1]["row_hash"]
    assert df_h[0]["row_hash"] != df_h[2]["row_hash"]


def test_dedup_by_order_keeps_latest(make_df):
    df = make_df(
        [(1, "old", "2024-01-01"),
         (1, "new", "2024-01-15"),
         (2, "only", "2024-01-10")],
        "id long, val string, updated_at string",
    )
    out = deduplicate_by_order(df, ["id"], "updated_at DESC NULLS LAST").collect()
    rows = {r["id"]: r["val"] for r in out}
    assert rows == {1: "new", 2: "only"}


def test_dedup_no_keys_returns_input(make_df):
    df = make_df([(1,)], "id long")
    assert deduplicate_by_order(df, [], "id DESC") is df


def test_dedup_invalid_order_expr(make_df):
    df = make_df([(1,)], "id long")
    with pytest.raises(ValueError, match="ordenação"):
        deduplicate_by_order(df, ["id"], "  ,  ")


def test_build_custom_keys(make_df):
    df = make_df(
        [(1, "AB", "X"), (2, None, "Y")],
        "id long, code string, region string",
    )
    out = build_custom_keys(df, {"composite_key": ["code", "region"]})
    rows = {r[0] for r in out.select("composite_key").collect()}
    assert rows == {"|Y", "AB|X"}  # null -> ""


def test_fix_encoding_disabled_passthrough(make_df):
    df = make_df([("x",)], "v string")
    assert fix_encoding(df, False, "Windows-1252", []) is df


def test_validate_schema_policy_new_table(make_df):
    df = make_df([(1,)], "id long")
    result = validate_schema_policy(df, "ops.does_not_exist_zzz", "permissive")
    assert result["status"] == "new_table"


def test_validate_schema_policy_strict_violation(spark, make_df, unique_name):
    table = f"bronze.{unique_name}_strict"
    spark.sql(f"CREATE TABLE {table} (id LONG, name STRING) USING DELTA")
    df = make_df([(1, "x", 99)], "id long, name string, extra long")
    with pytest.raises(ValueError, match="strict"):
        validate_schema_policy(df, table, "strict")


def test_validate_schema_policy_additive_only_blocks_removal(spark, make_df, unique_name):
    table = f"bronze.{unique_name}_add"
    spark.sql(f"CREATE TABLE {table} (id LONG, name STRING) USING DELTA")
    df = make_df([(1,)], "id long")  # 'name' removido
    with pytest.raises(ValueError, match="additive_only"):
        validate_schema_policy(df, table, "additive_only")


def test_validate_schema_policy_additive_allows_added(spark, make_df, unique_name):
    table = f"bronze.{unique_name}_add_ok"
    spark.sql(f"CREATE TABLE {table} (id LONG) USING DELTA")
    df = make_df([(1, "x")], "id long, name string")
    result = validate_schema_policy(df, table, "additive_only")
    assert result["added_columns"] == ["name"]
    sync_delta_schema(df, table, result, "additive_only")
    cols = [f.name for f in spark.table(table).schema.fields]
    assert "name" in cols


def test_table_exists(spark, unique_name):
    name = f"bronze.{unique_name}_exists"
    assert not table_exists(name)
    spark.sql(f"CREATE TABLE {name} (id LONG) USING DELTA")
    assert table_exists(name)
