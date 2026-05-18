# 5-Minute Quick Start

This guide shows the smallest functional flow to validate ContractForge without building a complete data platform structure.

## 1. Install

On Databricks, install a versioned wheel on the cluster or job:

```bash
%pip install /Volumes/<catalog>/<schema>/libs/contractforge-2.12.0-py3-none-any.whl
```

For local development:

```bash
pip install -e ".[dev]"
```

For local Spark/Delta execution:

```bash
pip install ".[spark]"
```

## 2. Create a Simple Source Table

In a Databricks notebook:

```python
from pyspark.sql import Row

spark.sql("CREATE SCHEMA IF NOT EXISTS main.raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.ops")

df = spark.createDataFrame([
    Row(order_id=1, customer_id=10, amount=120.5, updated_at="2026-05-14T10:00:00Z"),
    Row(order_id=2, customer_id=20, amount=75.0, updated_at="2026-05-14T10:05:00Z"),
])

df.write.mode("overwrite").saveAsTable("main.raw.orders_quickstart")
```

## 3. Run a Minimal Ingestion

```python
from contractforge import ingest

result = ingest(
    source={
        "type": "connector",
        "connector": "table",
        "table": "main.raw.orders_quickstart",
        "read": {"source_complete": True},
    },
    catalog="main",
    layer="bronze",
    target_table="b_orders_quickstart",
    mode="scd0_append",
    schema_policy="additive_only",
    ctrl_schema="ops",
    quality_rules={
        "not_null": ["order_id"],
        "unique_key": ["order_id"],
    },
)

result
```

Expected result:

- `status = SUCCESS`
- `target_table = main.bronze.b_orders_quickstart`
- `rows_read = 2`
- `rows_written = 2`

If the execution fails, ContractForge raises `ContractForgeExecutionError` by default after persisting the failure in the control tables. To inspect a failed result payload without raising, pass `raise_on_failure=False`.

By default, the physical target schema is the `layer` value. If your organization uses business schemas, add `target_schema`, for example `target_schema="landing_orders"`, to write to `main.landing_orders.b_orders_quickstart` while keeping `layer="bronze"` as logical metadata.

## 4. Inspect Operational Evidence

```sql
SELECT run_id, target_table, mode, status, rows_read, rows_written, framework_version
FROM main.ops.ctrl_ingestion_runs
WHERE target_table = 'main.bronze.b_orders_quickstart'
ORDER BY run_ts_utc DESC
LIMIT 10;
```

```sql
SELECT target_table, rule_name, status, failed_count
FROM main.ops.ctrl_ingestion_quality
WHERE target_table = 'main.bronze.b_orders_quickstart'
ORDER BY checked_at_utc DESC;
```

## 5. Move to YAML

After the minimal flow succeeds, move the ingestion intent to a YAML contract:

```yaml
source:
  type: connector
  connector: table
  table: main.raw.orders_quickstart
  read:
    source_complete: true

catalog: main
layer: bronze
target_table: b_orders_quickstart
mode: scd0_append
schema_policy: additive_only
ctrl_schema: ops

quality_rules:
  not_null: [order_id]
  unique_key: [order_id]
```

Validate before running:

```bash
contractforge validate contracts/bronze/b_orders_quickstart.ingestion.yaml
```
