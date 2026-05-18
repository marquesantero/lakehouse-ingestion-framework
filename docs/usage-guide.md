# Usage Guide

This guide is written for data engineers and analytics engineers who want to use ContractForge in notebooks, Databricks jobs or Databricks Asset Bundles. It focuses on daily usage, project structure, contracts, execution patterns and operational evidence.

For a browsable product manual, use the documentation site:

https://marquesantero.github.io/contractforge/

For internals and contribution details, see [Architecture](architecture.md).

## What ContractForge Does

ContractForge standardizes ingestion into Delta Lake through contracts. A contract describes the source, target, write mode, schema policy, quality rules, transformations, governance metadata and operational behavior for one table.

Instead of copying similar PySpark code across notebooks, users define the intent and let the library execute a consistent pattern:

1. Resolve the source.
2. Apply declarative preparation and transformations.
3. Validate schema and quality.
4. Write with a known Delta semantics.
5. Persist operational evidence in control tables.
6. Optionally apply annotations and governance metadata.

ContractForge does not replace an orchestrator. Use Databricks Workflows, DAB, Airflow or another scheduler to decide when and in which order contracts run.

## Installation

Install from PyPI when running outside a packaged Databricks job:

```bash
pip install contractforge
```

Install optional dependencies only when you need them:

```bash
pip install "contractforge[spark]"
pip install "contractforge[aws]"
```

In Databricks, prefer a versioned wheel installed as a job or cluster library. If you use notebook-scoped installs, restart Python after installation:

```python
%pip install contractforge==2.12.0
dbutils.library.restartPython()
```

## First Run With Python

Use the Python API when the source DataFrame is already built in the notebook or when a transformation is intentionally too custom for a reusable contract.

```python
from contractforge import ingest

result = ingest(
    source=df,
    catalog="main",
    target_schema="sales_curated",
    layer="silver",
    target_table="s_orders",
    mode="scd1_upsert",
    merge_keys=["order_id"],
    watermark_columns=["updated_at"],
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "not_null": ["order_id", "updated_at"],
        "unique_key": ["order_id"],
    },
)
```

By default, a failed execution raises `ContractForgeExecutionError` after the failed run has been recorded in the control tables. Use `raise_on_failure=False` when you intentionally need the failed result payload:

```python
result = ingest(..., raise_on_failure=False)
if result["status"] != "SUCCESS":
    display(result)
```

## First Run With YAML

YAML contracts are recommended for repeatable ingestion jobs because they are reviewable, diffable and reusable by generic notebooks.

```yaml
preset: silver_scd1_upsert

source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    dbtable: public.orders
  auth:
    type: basic
    username: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"

target:
  catalog: main
  schema: sales_curated
  table: s_orders

layer: silver
merge_keys: [order_id]
watermark_columns: [updated_at]
dedup_order_expr: "updated_at DESC NULLS LAST"
schema_policy: additive_only

quality_rules:
  not_null: [order_id, updated_at]
  unique_key: [order_id]
```

Run it from a generic notebook:

```python
import yaml
from contractforge import ingest

contract_path = dbutils.widgets.get("contract_path")
with open(contract_path, "r", encoding="utf-8") as handle:
    contract = yaml.safe_load(handle)

result = ingest(**contract)
display(result)
```

## Recommended Project Layout

Use a layout that separates contracts, reusable notebooks, dashboards and workflow definitions:

```text
project/
  contracts/
    bronze/
      b_orders.ingestion.yaml
      b_orders.operations.yaml
    silver/
      s_orders.ingestion.yaml
      s_orders.annotations.yaml
      s_orders.operations.yaml
    gold/
      g_daily_sales.ingestion.yaml
      g_daily_sales.annotations.yaml
      g_daily_sales.access.yaml
  notebooks/
    run_contract.py
    run_layer.py
  resources/
    jobs.yml
  dashboards/
  tests/
```

Use one ingestion contract per target table. Split governance files when ownership differs:

- `*.ingestion.yaml`: engineering-owned execution contract.
- `*.annotations.yaml`: catalog comments, tags, aliases and PII metadata.
- `*.operations.yaml`: owners, criticality, SLA and runbook metadata.
- `*.access.yaml`: grants, row filters and column masks.

## Target Naming

The physical target table is resolved as:

```text
{catalog}.{target_schema or layer}.{target_table}
```

`layer` is operational classification. It can be `bronze`, `silver`, `gold`, `stage`, `raw`, `trusted`, `curated` or any valid logical value. Use `target_schema` or `target.schema` when the physical schema should not be the same as the layer.

```yaml
layer: stage
target:
  catalog: main
  schema: ingestion_stage
  table: stg_orders
```

## Choosing a Write Mode

Use the mode that matches the business semantics of the target table.

| Mode | Use when | Required keys |
| --- | --- | --- |
| `scd0_append` | Immutable event or landing table. | None |
| `scd0_overwrite` | Full refresh or controlled partition replacement. | Optional partition fields |
| `scd1_upsert` | Current-state table with update by key. | `merge_keys` |
| `scd1_hash_diff` | Append only when non-key values change. | `hash_keys` |
| `scd2_historical` | Keep historical versions with current flag. | `merge_keys` |
| `snapshot_soft_delete` | Source is a complete snapshot and missing keys must be deactivated. | `merge_keys` |

Do not use `snapshot_soft_delete` with watermarks or filters. A partial source cannot prove that a missing record was deleted.

## Connectors

Connectors describe how the source is read. They do not model business transformations. Use `transform.shape` for structural transformations after reading.

Common source examples:

```yaml
source:
  type: connector
  connector: http_file
  format: csv
  path: https://example.org/dataset.csv
  read:
    header: true
    inferSchema: false
    schema: "id STRING, updated_at TIMESTAMP, amount DOUBLE"
```

```yaml
source:
  type: connector
  connector: s3
  path: s3a://company-landing/orders/
  format: json
  read:
    schema: "id STRING, payload STRING, updated_at TIMESTAMP"
    recursiveFileLookup: true
    pathGlobFilter: "*.json"
```

```yaml
source:
  type: connector
  connector: jdbc
  options:
    url: "{{ secret:warehouse/jdbc_url }}"
    dbtable: public.orders
  auth:
    type: basic
    username: "{{ secret:warehouse/user }}"
    password: "{{ secret:warehouse/password }}"
  read:
    partitionColumn: id
    lowerBound: 1
    upperBound: 1000000
    numPartitions: 8
    fetchsize: 10000
```

For object storage on Databricks Serverless, prefer Unity Catalog external locations. Direct credential-based Spark configuration is runtime-dependent and usually more appropriate on classic clusters.

## Transformations With `transform.shape`

Use `transform.shape` when a connector returns nested JSON, structs or arrays and the target table needs a curated shape.

```yaml
transform:
  shape:
    flatten:
      - source: properties
        prefix: property_
    columns:
      event_id:
        source: id
        cast: string
      event_time:
        source: properties.time
        cast: timestamp
      longitude:
        source: geometry.coordinates[0]
        cast: double
      latitude:
        source: geometry.coordinates[1]
        cast: double
    arrays:
      readings:
        mode: explode_outer
        alias: reading
```

Keep Bronze tables close to the source unless exploding arrays is an explicit ingestion requirement. Normalize nested structures in Silver when possible.

## Quality Rules

Quality rules are pipeline gates, not a full data quality catalog. They answer whether the data can be written safely.

```yaml
quality_rules:
  required_columns: [order_id, updated_at, amount]
  not_null: [order_id, updated_at]
  unique_key: [order_id]
  min_rows: 1
  max_null_ratio:
    amount: 0.05
  accepted_values:
    status: [open, closed, cancelled]
  expressions:
    - name: positive_amount
      expression: "amount >= 0"
      severity: quarantine
      message: "Amount must be non-negative."
```

Rules such as `unique_key`, `required_columns` and `min_rows` describe the whole dataset and cannot isolate a single bad row. If they fail while `on_quality_fail=quarantine`, ContractForge escalates to failure.

## Schema Policy

Choose schema policy according to the trust level of the table:

| Policy | Behavior |
| --- | --- |
| `permissive` | Allows broad schema movement. Best for raw landing. |
| `additive_only` | Allows new columns but blocks removals and unsafe type changes. |
| `strict` | Requires source and target schemas to match. |

Use `allow_type_widening=true` only when widening is an intentional contract decision.

## Watermarks and Backfill

Use `watermark_columns` for incremental reads and stateful progress tracking. Watermarks are stored in `ctrl_ingestion_state`.

```yaml
watermark_columns: [updated_at]
```

For composite ordering:

```yaml
watermark_columns: [event_date, event_sequence]
dedup_order_expr: "event_date DESC NULLS LAST, event_sequence DESC NULLS LAST"
```

For controlled historical processing, use execution windows or catchup configuration instead of changing the main watermark manually.

## Idempotency, Retry and Locks

Use idempotency for jobs that may be retried by an orchestrator:

```yaml
idempotency_key: "orders:2026-05-17"
idempotency_policy: skip_if_success
```

Supported policies:

- `always_run`: always execute.
- `skip_if_success`: skip if the same key already succeeded.
- `fail_if_success`: fail if the same key already succeeded.
- `rerun_if_failed`: rerun only when the previous run failed.

Use per-plan retry settings for tables with known Delta contention:

```yaml
retry_attempts: 5
retry_backoff_seconds: 10
```

Locks are cooperative and best-effort. They reduce accidental concurrent writes by ContractForge jobs but do not protect against non-ContractForge writers.

## Operations Metadata

Use operations contracts to make run ownership and SLA visible in control tables:

```yaml
operations:
  business_owner: sales-ops
  technical_owner: data-platform
  support_group: data-platform
  criticality: high
  expected_frequency: daily
  freshness_sla_minutes: 180
  alert_on_failure: true
  alert_on_quality_fail: true
  runbook_url: https://wiki.example.com/runbooks/orders
```

ContractForge records this metadata; alert routing is intentionally left to dashboards, SQL alerts or external monitoring systems.

## Annotations and Access

Use annotations to describe tables and fields:

```yaml
annotations:
  table:
    description: Curated orders table.
    tags:
      domain: sales
      quality: curated
  columns:
    customer_email:
      description: Customer contact email.
      pii:
        enabled: true
        type: email
        sensitivity: restricted
      tags:
        confidentiality: restricted
```

Use access contracts when you want declarative grants or Unity Catalog policies:

```yaml
access:
  grants:
    - principal: account users
      privileges: [SELECT]
```

Access is usually applied by a dedicated governance command or pipeline, not necessarily by the ingestion task itself.

## Control Tables

The `ctrl_schema` stores operational evidence. Common tables:

| Table | Purpose |
| --- | --- |
| `ctrl_ingestion_runs` | One row per execution, with status, target, metrics and stage durations. |
| `ctrl_ingestion_state` | Current watermark and state per target. |
| `ctrl_ingestion_quality` | Quality rule results. |
| `ctrl_ingestion_quarantine` | Rejected rows for quarantine-capable rules. |
| `ctrl_ingestion_errors` | Full redacted stack traces. |
| `ctrl_ingestion_streams` | Auto Loader available-now parent stream metrics. |
| `ctrl_ingestion_annotations` | Applied or skipped catalog annotations. |
| `ctrl_ingestion_operations` | Ownership, criticality and SLA metadata. |
| `ctrl_ingestion_access` | Access governance actions and drift information. |

Example run inspection:

```sql
SELECT
  started_at_utc,
  status,
  target_table,
  source_connector,
  rows_read,
  rows_written,
  error_message
FROM ops.ctrl_ingestion_runs
ORDER BY started_at_utc DESC
LIMIT 50;
```

## Databricks Workflow Pattern

Recommended production pattern:

1. Store contracts in Git.
2. Deploy contracts and notebooks through Databricks Asset Bundles.
3. Use one generic runner notebook.
4. Pass `contract_path` as a task parameter.
5. Use Workflow retry for infrastructure failures and ContractForge idempotency for application-level safety.

For many independent tables, use a `for_each_task` or generate tasks from a contract inventory. Keep dependencies explicit between Bronze, Silver and Gold layers.

## Validation Before Running

Use the CLI to validate contracts without starting Spark:

```bash
contractforge validate contracts/silver/s_orders.ingestion.yaml
contractforge schema > contractforge.schema.json
contractforge templates list
contractforge templates wizard --layer silver --source jdbc --mode scd1_upsert
```

Validation catches structural errors, unsupported enum values and obvious contract mistakes. It cannot verify source connectivity or target permissions without executing in the runtime.

## Troubleshooting

Start with the returned payload or `ctrl_ingestion_runs`:

```sql
SELECT *
FROM ops.ctrl_ingestion_runs
WHERE run_id = '<run_id>';
```

Then inspect full errors:

```sql
SELECT stack_trace
FROM ops.ctrl_ingestion_errors
WHERE run_id = '<run_id>'
ORDER BY error_ts_utc DESC;
```

Common causes:

- Source credentials are unavailable in the selected runtime.
- Serverless requires external locations or network policy for object storage.
- A schema policy blocks a type change.
- A quality rule failed and `on_quality_fail=fail`.
- `snapshot_soft_delete` was configured with a partial source.
- JDBC partition bounds do not match the actual column distribution.

## Production Checklist

Before adopting a contract in a scheduled job:

- The contract is stored in Git and reviewed.
- Secrets use `{{ secret:scope/key }}` placeholders.
- `target.schema` is explicit when physical schema should not equal `layer`.
- The write mode matches the business semantics.
- `merge_keys`, `hash_keys` and `dedup_order_expr` are deterministic.
- Quality rules cover primary keys and minimum viability.
- Schema policy is strict enough for the layer.
- Idempotency policy is defined for retryable jobs.
- Operations metadata includes owner, support group and runbook.
- Control table access is restricted.
- Quarantine tables follow the same sensitivity policy as source data.
