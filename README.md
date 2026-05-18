<p align="center">
  <img src="docs/assets/logo/contractforge-logo.png" alt="ContractForge" width="520">
</p>

# ContractForge

**Contract-first ingestion, governance and observability for Databricks and Delta Lake.**

ContractForge turns recurring ingestion patterns into versioned contracts. Instead of spreading read logic, schema evolution, quality gates, write modes, catalog annotations, access rules and operational metadata across ad-hoc notebooks, you declare the intent in YAML or Python and let the framework execute the standard operating pattern.

It is designed for teams that want the governance discipline of declarative pipelines without losing the control of regular Spark/Delta jobs.

## Start Here

- **Web documentation:** https://marquesantero.github.io/contractforge/
- **Quick start:** [docs/quickstart.md](docs/quickstart.md)
- **Documentation map:** [docs/README.md](docs/README.md)
- **Usage guide:** [docs/usage-guide.md](docs/usage-guide.md)
- **Architecture:** [docs/architecture.md](docs/architecture.md)
- **Reference map:** [docs/reference.md](docs/reference.md)
- **Project template:** [examples/project_template](examples/project_template)
- **Example playground:** [examples/playground](examples/playground)
- **Templates:** [docs/templates.md](docs/templates.md)
- **Connector compatibility:** [docs/connector-compatibility.md](docs/connector-compatibility.md)
- **Changelog:** [CHANGELOG.md](CHANGELOG.md)

## Why ContractForge

Modern lakehouse ingestion usually starts simple and then accumulates hidden operational rules: retries, watermarks, schema drift, SCD semantics, quarantines, lineage, secrets, access grants and dashboard evidence. ContractForge keeps those rules explicit and reviewable.

Core capabilities:

- Declarative ingestion through YAML contracts or Python calls.
- Logical layers such as `bronze`, `silver`, `gold`, `stage`, `raw` or any custom classification.
- Independent physical schema control through `target.schema` / `target_schema`.
- Official write modes: append, overwrite, SCD1 upsert, hash-diff append, SCD2 history and snapshot soft delete.
- Built-in quality gates, quarantine, schema policies, typed watermarks, idempotency, locks and retry.
- Control tables for runs, state, quality, quarantine, errors, locks, lineage, explain plans, schema changes, streams, annotations, operations and access.
- Declarative governance through split contracts: `*.ingestion.yaml`, `*.annotations.yaml`, `*.operations.yaml` and `*.access.yaml`.
- Source connectors for tables, SQL, files, HTTP files, object storage, JDBC, REST APIs, Auto Loader `available_now`, Snowflake and BigQuery.
- Runtime-aware behavior for Databricks classic clusters, serverless/Spark Connect and local Spark/Delta.
- Secret redaction in source metadata, lineage, tracebacks and persisted error records.

## Positioning

ContractForge does not try to replace Delta Live Tables/Lakeflow as a managed orchestration product. It complements Databricks jobs, notebooks and Databricks Asset Bundles with table-level contracts, predictable write semantics and auditable control tables.

Use ContractForge when you need:

- Fine-grained control per table.
- Declarative contracts that can be reviewed in pull requests.
- Portable Spark/Delta execution patterns.
- Governance evidence in Delta tables.
- A bridge between notebooks, jobs, templates and enterprise data governance.

## Installation

The PyPI package and Python namespace are both named `contractforge`.

```bash
pip install contractforge
```

For local development:

```bash
pip install -e ".[dev]"
```

For standalone Spark/Delta outside Databricks:

```bash
pip install ".[spark]"
```

For optional AWS credential provider support, for example RDS IAM default credential chain:

```bash
pip install "contractforge[aws]"
```

On Databricks, the wheel does not require `pyspark` or `delta-spark` because those are provided by the runtime.

## Quick Example: Python

```python
from contractforge import ingest

result = ingest(
    source=df,
    target_table="s_orders",
    catalog="main",
    layer="silver",
    target_schema="sales_curated",
    mode="scd1_upsert",
    merge_keys="order_id",
    column_mapping={"id": "order_id"},
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "not_null": ["order_id"],
        "unique_key": ["order_id"],
    },
)
```

Execution failures raise `ContractForgeExecutionError` by default after the run has been recorded in control tables. If you need the legacy-style failed payload for diagnostics or tests, call `ingest(..., raise_on_failure=False)`.

## Quick Example: YAML Contract

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
schema_policy: additive_only

quality_rules:
  not_null: [order_id]
  unique_key: [order_id]
```

`layer` is operational metadata. The physical destination is controlled by `target.schema` or `target_schema`; if omitted, ContractForge uses `layer` as the fallback schema.

## Contract Bundles

For real projects, keep responsibilities separated:

```text
contracts/gold/gd_orders.ingestion.yaml
contracts/gold/gd_orders.annotations.yaml
contracts/gold/gd_orders.operations.yaml
contracts/gold/gd_orders.access.yaml
```

Each file has a different review owner:

- `ingestion`: source, target, write mode, schema policy, quality, watermark and transforms.
- `annotations`: table/column descriptions, aliases, tags and PII metadata.
- `operations`: business owner, technical owner, support group, criticality, SLA and runbook.
- `access`: grants, row filters and column masks.

Validate a bundle before running it:

```bash
contractforge validate-bundle contracts/gold/gd_orders
contractforge governance-preview contracts/gold/gd_orders
```

## Declarative Transforms

Use `transform` for physical transformations before quality checks and writes.

`transform.shape` normalizes JSON, structs and arrays without embedding PySpark in every notebook:

```yaml
transform:
  shape:
    zip_arrays:
      - alias: hourly_rows
        columns:
          hourly.time: time
          hourly.temperature_2m: temperature_2m
    arrays:
      - path: hourly_rows
        mode: explode_outer
        alias: hour
    columns:
      location_id: location_id
      hour.time: forecast_hour
      hour.temperature_2m:
        alias: temperature_2m
        cast: DOUBLE
      forecast_date:
        alias: forecast_date
        expression: "TO_DATE(hour.time)"
```

`transform.deduplicate` resolves repeated source rows before MERGE:

```yaml
transform:
  deduplicate:
    keys: [order_id]
    order_by: "updated_at DESC NULLS LAST, ingestion_sequence DESC"
```

## Source Connectors

Contracts can resolve sources without custom notebook code.

| Source | Typical use |
| --- | --- |
| `table`, `delta_table`, `view`, `sql` | Existing lakehouse objects |
| `csv`, `json`, `parquet`, `avro`, `orc`, `delta`, `text`, `xml` | Spark file readers |
| `http_file`, `http_csv`, `http_json`, `http_text` | Public or authenticated HTTP(S) files materialized by the driver |
| `s3`, `azure_blob`, `object_storage`, `blob` | Object storage through Spark filesystem access |
| `jdbc`, `postgres`, `mysql`, `sqlserver`, `oracle` | JDBC sources with optional auth blocks |
| `rest_api` | Paginated APIs with secrets, limits and incremental parameters |
| `autoloader` | Databricks Auto Loader `available_now` |
| `snowflake`, `bigquery` | External Spark connectors installed on the runtime |

Inspect connector details locally:

```bash
contractforge connectors show rest_api http_file postgres s3 autoloader
contractforge connectors doctor rest_api http_file postgres s3 autoloader
```

## Object Storage Guidance

On Databricks serverless/Spark Connect, prefer Unity Catalog External Locations or Volumes for Azure Blob, ADLS and S3. Credentials and network access are then governed by Unity Catalog.

On classic/job clusters or local Spark, ContractForge can configure some credentials declaratively, such as Azure Blob SAS or S3A static/temporary credentials.

S3 example:

```yaml
source:
  type: connector
  connector: s3
  path: s3a://company-landing/orders/
  format: csv
  auth:
    access_key_id: "{{ secret:aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:aws/aws_secret_access_key }}"
    session_token: "{{ secret:aws/aws_session_token }}" # optional
  options:
    header: true
    fs.s3a.endpoint: s3.us-east-1.amazonaws.com
  read:
    source_complete: true
    schema: "order_id STRING, customer_id STRING, amount DOUBLE"
```

For known schemas, declare `source.read.schema` using Spark DDL. `source.schema` is accepted as a short alias and normalized to `source.read.schema`.

For many files, prefer Spark-native `pathGlobFilter` when possible. Use `source.read.file_regex` only when true regex filtering is needed, because recursive object storage listings can be expensive.

## HTTP Files and REST APIs

Use `http_file` when the source is a file exposed over HTTP(S) and Spark cannot read the URL directly as a filesystem path.

```yaml
source:
  type: connector
  connector: http_file
  path: https://example.com/public/orders.csv
  format: csv
  options:
    header: true
  read:
    source_complete: true
    schema: "order_id STRING, order_date DATE, amount DOUBLE"
```

For large or complex REST JSON responses, keep the connector responsible for download and use `transform.shape` for parsing:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://api.example.com/events
  response:
    mode: raw
    raw_column: raw_response
  limits:
    max_page_bytes: 10485760
    max_total_bytes: 52428800

transform:
  shape:
    parse_json:
      - column: raw_response
        alias: payload
        schema: "STRUCT<events: ARRAY<STRUCT<id: STRING, title: STRING>>>"
```

## JDBC and RDS IAM

JDBC connectors support `auth.type: basic` and Amazon RDS/Aurora IAM database authentication through `auth.type: rds_iam`.

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: jdbc:postgresql://orders.cluster-xyz.us-east-1.rds.amazonaws.com:5432/app
    dbtable: public.orders
    driver: org.postgresql.Driver
  auth:
    type: rds_iam
    username: "{{ secret:aws-rds/db_user }}"
    region: us-east-1
    credential_provider: default_chain
  read:
    fetchsize: 10000
    partition_column: id
    lower_bound: 1
    upper_bound: 10000000
    num_partitions: 8
```

Network connectivity is still the runtime responsibility: same VPC, peering, Transit Gateway, PrivateLink/NLB, public endpoint rules or equivalent setup. See [RDS/Aurora JDBC with IAM Auth](docs/rds_iam_jdbc.md).

## Backfill and Catchup Windows

Use `execution.window` to split historical loads into traceable child runs. Each window applies a `[start, end)` filter and writes child rows to `ctrl_ingestion_runs` with a shared `parent_run_id`.

```yaml
execution:
  window:
    column: updated_at
    start: "2026-05-01T00:00:00"
    end: "2026-05-08T00:00:00"
    every: "1 day"
    stop_on_failure: true
```

Use `execution.catchup` to generate windows from the saved watermark when `start` is omitted:

```yaml
watermark_columns: [updated_at]
execution:
  catchup:
    enabled: true
    column: updated_at
    end: "2026-05-17T00:00:00"
    every: "1 day"
```

## Templates

Templates generate complete starter bundles. They are based on patterns validated with real ingestion examples.

```bash
contractforge templates list
contractforge templates wizard --layer bronze --source http_file --pattern csv
contractforge templates wizard --layer silver --source jdbc --pattern rds_iam
contractforge templates write silver_jdbc_rds_iam_hash_diff --output contracts/silver/s_orders_hash_diff
```

Examples include:

- `bronze_http_file_csv_snapshot`
- `bronze_object_storage_nested_json_shape`
- `bronze_object_storage_small_files`
- `bronze_autoloader_available_now_json`
- `silver_jdbc_rds_iam_hash_diff`
- `silver_raw_json_payload_shape`
- `silver_parallel_arrays_shape`
- `gold_full_refresh_kpi`

See [Templates](docs/templates.md) for the full catalog.

## Operational Tooling

Control table retention:

```bash
contractforge maintenance ctrl-retention --catalog main --ctrl-schema ops --retention-days 180
contractforge maintenance ctrl-retention --catalog main --ctrl-schema ops --retention-days 180 --vacuum --apply
```

Estimated operational cost and throughput:

```bash
contractforge maintenance cost-report \
  --catalog main \
  --ctrl-schema ops \
  --lookback-days 30 \
  --group-by contract_domain \
  --group-by criticality \
  --dbu-per-hour 2.5 \
  --currency-per-dbu 0.55
```

The cost report is an operational estimate from `ctrl_ingestion_runs.duration_seconds`; it is not cloud billing data.

## CLI Overview

```bash
contractforge init --output contracts/silver/s_orders --source raw.orders --target-table s_orders --layer silver --target-schema sales_curated --mode scd1_upsert --merge-keys order_id --split
contractforge validate contracts/silver/s_orders.ingestion.yaml
contractforge validate-bundle contracts/silver/s_orders
contractforge validate-project contracts
contractforge governance-preview contracts/silver/s_orders
contractforge governance-check contracts/silver/s_orders
contractforge templates list
contractforge presets list
contractforge connectors doctor postgres rest_api http_file s3
```

## Development

```bash
pip install -e ".[dev]"
pytest
python scripts/check_release.py
```

Before opening a pull request, read [CONTRIBUTING.md](CONTRIBUTING.md). The `main` branch is protected and expects PR review, resolved conversations and passing checks for `build`, `test (3.10)` and `test (3.11)`.

Release checklist:

```bash
python -m build
twine check dist/*
git tag vX.Y.Z
git push origin vX.Y.Z
```

## License

MIT. See [LICENSE](LICENSE).
