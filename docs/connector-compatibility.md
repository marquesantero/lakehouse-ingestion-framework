# Connector Compatibility Matrix

This matrix describes the connector contract supported by ContractForge. Drivers, credentials, external locations and external Spark libraries remain runtime responsibilities.

| Connector | Expected runtime | External dependency | Local Spark | Databricks classic | Databricks serverless | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `table`, `delta_table`, `view` | Spark catalog | Spark/Delta only | Yes | Yes | Yes | Depends on catalog/schema/table permissions. |
| `sql` | Spark SQL | Spark only | Yes | Yes | Yes | Use for traceable, versioned queries; avoid very large SQL strings in YAML. |
| `parquet`, `json`, `jsonl`, `ndjson`, `csv`, `orc`, `text`, `avro`, `xml` | Spark file reader | Runtime Hadoop/Spark connectors | Yes | Yes | Yes | Path and credentials must be accessible to Spark; `jsonl`/`ndjson` use the JSON reader; `xml` depends on runtime support. |
| `http_file` | Python driver | Standard library `urllib` | Yes | Yes | Yes | Downloads HTTP(S) on the driver and creates a DataFrame; use `format=csv|json|jsonl|ndjson|text`. |
| `http_csv`, `http_json`, `http_text` | Python driver | Standard library `urllib` | Yes | Yes | Yes | Aliases for `http_file`; useful when Spark cannot read `https://` as a filesystem path. |
| `delta` | Spark Delta reader | Delta Lake | Yes with `spark` extra | Yes | Yes | Path-based; prefer `delta_table`/`table` for registered tables. |
| `object_storage`, `blob` | Spark file reader | Configured cloud credentials | Partial | Yes | Yes | Use `provider=adls|azure_blob|s3|gcs`; Azure Blob SAS can be declared in `auth.sas_token`; S3 keys can be declared in `auth` for classic/local runtimes. |
| `s3` | Spark file reader | S3 runtime access or `source.auth` in classic/local | Partial | Yes | Yes via External Location; direct auth may be blocked | Alias for object storage with inferred provider; `source.auth.access_key_id`, `secret_access_key` and optional `session_token` configure `fs.s3a.*`. |
| `adls`, `azure_blob` | Spark file reader | Azure Storage access through runtime/Unity Catalog or SAS in runtimes that allow Hadoop config | Partial | Yes | Yes through External Location/Volume or allowed networking | `azure_blob` accepts `account_url`, `container` and `auth.sas_token`; if serverless blocks `fs.azure.sas...`, ContractForge fails fast with operational guidance. |
| `gcs` | Spark file reader | GCS access in the runtime | Partial | Yes | Yes | Requires GCS configuration in the cluster/serverless runtime. |
| `jdbc` | Spark JDBC | JDBC driver | Yes | Yes | Yes, if driver/runtime supports it | Requires `options.url` and `dbtable` or `query`; accepts `source.auth` for basic/RDS IAM. |
| `postgres`, `postgresql` | Spark JDBC | PostgreSQL driver | Yes | Yes | Yes, if driver is available | Alias for `jdbc`; supports `auth.type=rds_iam` for Amazon RDS/Aurora. |
| `sqlserver` | Spark JDBC | Microsoft SQL Server driver | Yes | Yes | Yes, if driver is available | Use `fetchsize` and partitioning for large tables. |
| `mysql` | Spark JDBC | MySQL/MariaDB driver | Yes | Yes | Yes, if driver is available | Alias for `jdbc`. |
| `oracle` | Spark JDBC | Oracle driver | Yes | Yes | Yes, if driver is available | Driver distribution is often controlled by licensing. |
| `rest_api` | Python driver | Standard library `urllib` | Yes | Yes | Yes | Suitable for paginated APIs with controlled volume. |
| `snowflake` | Spark connector | Spark Snowflake connector | Yes, if installed | Yes, if installed | Yes, if supported by the runtime | Delegates to `spark.read.format("snowflake")`. |
| `bigquery` | Spark connector | Spark BigQuery connector | Yes, if installed | Yes, if installed | Yes, if supported by the runtime | Delegates to `spark.read.format("bigquery")`. |
| `autoloader` | Databricks Auto Loader | Databricks Runtime | No | Yes | Yes | `available_now` only; continuous streaming is out of scope. |

## Practical Rules

- For recurring files or high volume, prefer `autoloader` on Databricks.
- For small or medium public HTTP(S) files, prefer `http_file` instead of direct `spark.read` on `https://`, especially on serverless.
- For large REST APIs, land files first and ingest them with `autoloader`.
- For `snapshot_soft_delete`, set `source.read.source_complete=true` only when the source represents the complete current state.
- For file filters, prefer `pathGlobFilter` when Spark globbing is enough. Use `source.read.file_regex` only when you need real regex filtering on `filename` or `relative_path`; ContractForge lists files through the Spark/Hadoop filesystem and applies `source.read.file_regex_max_listed`.
- For large JDBC tables, configure `partition_column`, `lower_bound`, `upper_bound`, `num_partitions` and `fetchsize`.
- For Amazon RDS/Aurora, network connectivity is not solved by the library: use the same VPC, VPC peering, Transit Gateway, PrivateLink/NLB or a traditional public endpoint. Aurora created by Express Configuration may use Internet Access Gateway with IAM token, but still requires IAM permission and TCP reachability from the runtime.
- For Snowflake/BigQuery, validate the Spark connector in the runtime before using the contract in production.
- For connectors that use credentials, use `{{ secret:scope/key }}` and verify that `contractforge validate`/`connectors doctor` do not print literal secrets.
- For Azure Blob with SAS, store only the SAS token in the secret scope and declare `account_url`, `container` and `path` separately in the contract.
- For Azure Blob on Databricks serverless, prefer Unity Catalog External Location/Volume and `abfss://...` or `/Volumes/...` paths; direct SAS through `fs.azure.sas...` may be blocked by Spark Connect.
- For S3 on Databricks serverless, prefer Unity Catalog External Location/Volume. `source.auth` with S3 keys is for classic/job clusters/local runtimes where `fs.s3a.*` can be configured.
- If the source is an explicit small or medium HTTP(S) file, use `http_file`. Do not treat `azure_blob` as an implicit REST downloader.

## Validation Examples

```bash
contractforge connectors list
contractforge connectors show s3 postgres snowflake bigquery rest_api http_file
contractforge connectors doctor s3 postgres snowflake bigquery rest_api http_file
contractforge validate contracts/bronze/b_orders.ingestion.yaml
```

`connectors doctor` does not open network connections, create a SparkSession or validate credentials. It shows static connector requirements such as JDBC drivers, external Spark connectors, Auto Loader or cloud runtime configuration. Use it in PRs and diagnostic notebooks before running real ingestion jobs.

## HTTP File CSV Example

Use `http_file` when the source is a file published through HTTP(S), but the Spark runtime does not implement direct `https://` reads as a filesystem. The connector downloads the file with Python and materializes the DataFrame in Spark while keeping secrets and options redacted in control tables.

```yaml
source:
  type: connector
  connector: http_file
  path: https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv
  format: csv
  options:
    header: true
    nullValue: ""
  read:
    source_complete: true
  limits:
    timeout_seconds: 60
    retry_attempts: 3
    retry_backoff_seconds: 2

target:
  catalog: workspace
  schema: cf_examples_bronze
  table: b_covid_brazil_states

layer: bronze
mode: scd0_overwrite
source_system: covid19br_github
```

Equivalent aliases:

```yaml
source:
  type: connector
  connector: http_csv
  path: https://example.com/data.csv
  options:
    header: true
```

## Incremental JDBC Example

```yaml
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
  incremental:
    watermark_column: updated_at
  read:
    fetchsize: 10000
    partition_column: id
    lower_bound: 1
    upper_bound: 10000000
    num_partitions: 16

target:
  catalog: main
  schema: sales_curated
  table: s_orders

layer: silver
mode: scd1_upsert
merge_keys: order_id
watermark_columns: updated_at
```

## Amazon RDS/Aurora JDBC with IAM Auth

`auth.type: rds_iam` generates the IAM token in the Python driver at read time. The token is short-lived for opening the connection, but the established session remains valid. Use this path when the runtime already has AWS credentials through secrets, environment variables or the AWS credential provider chain.

The full PostgreSQL user, IAM policy, `rds-db:connect`, secrets, JDBC driver and troubleshooting setup is in [RDS/Aurora JDBC with IAM Auth](rds_iam_jdbc.md).

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: jdbc:postgresql://database-1.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:5432/postgres
    dbtable: public.orders
    driver: org.postgresql.Driver
  auth:
    type: rds_iam
    username: postgres
    region: us-east-1
    access_key_id: "{{ secret:contractforge-aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:contractforge-aws/aws_secret_access_key }}"
    session_token: "{{ secret:contractforge-aws/aws_session_token }}"
    sslmode: require
```

For instance profile, web identity or another `botocore` provider-chain mechanism, omit explicit keys and use:

```yaml
  auth:
    type: rds_iam
    username: postgres
    region: us-east-1
    credential_provider: default_chain
```

This mode requires `botocore` in the Python driver, for example by installing `contractforge[aws]`.

Recommended network alternatives:

- Databricks AWS and RDS in the same VPC or routable subnets.
- VPC peering or Transit Gateway between the Databricks VPC and the RDS VPC.
- AWS PrivateLink with NLB for cross-VPC/cross-account scenarios.
- Traditional public RDS endpoint with security group restricted to the Databricks egress CIDR/IP, only for controlled validation.
- Aurora Express Internet Access Gateway with IAM token when the cluster was created in that mode and the relay accepts TCP from the runtime.

Practical notes:

- On Unity Catalog `standard`/shared clusters, Maven libraries may require artifact allowlisting. If the JDBC driver is blocked, use a `SINGLE_USER` cluster for validation or ask the metastore admin for allowlisting.
- `PAM authentication failed` usually means the database user does not have `rds_iam`, the IAM principal lacks `rds-db:connect`, the token expired, or the token was generated for a different user/host/region.
- When using `ingest()` directly, pass `catalog` explicitly. A qualified `target_schema` does not replace `plan.catalog`.

## Incremental REST API Example

```yaml
source:
  type: connector
  connector: rest_api
  name: orders_api
  request:
    url: https://api.example.com/orders
    params:
      status: open
  auth:
    type: bearer_token
    token: "{{ secret:orders_api/token }}"
  pagination:
    type: cursor
    cursor_param: cursor
    next_cursor_path: $.next
  response:
    records_path: $.data
  incremental:
    watermark_param: updated_after
    watermark_header: X-Watermark
  limits:
    max_pages: 100
    timeout_seconds: 60
    retry_attempts: 3

target:
  catalog: main
  schema: bronze
  table: b_orders_api

layer: bronze
mode: scd0_append
watermark_columns: updated_at
```

## REST API with Complex JSON Payload

For nested JSON, arrays of structs or payloads with variable schema, `response.mode: records` can materialize real records more robustly on classic Spark by using JSON lines + `spark.read.json`, through RDD when available or through configured `source.read.staging_path`. The selected path is stored in `source_metrics.dataframe_materialization`.

`response.records_path` supports simple JSON navigation, not full JSONPath: use `$` for the root, `$.data.items` for fields, `$[1]` for an index in a root array and `$.data[0].items` for intermediate arrays. Wildcards, filters and expressions are not supported.

When the API returns dynamic objects or JSON that is too heterogeneous for safe inference, declare `source.read.schema`. `source.schema` is accepted as a short alias and normalized to `source.read.schema`; conflicts between the two fail before reading. The connector applies this DDL to the Spark JSON reader before reading materialized records. This is the recommended path for large public APIs instead of relying on automatic inference.

If the runtime does not expose `sparkContext`, declare staging that is accessible to both the Python driver and Spark reader:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://api.example.com/items
  response:
    records_path: $.data[0].items
  read:
    staging_path: /Volumes/main/ops/tmp/contractforge_rest_api
    schema: "id STRING, payload STRUCT<status:STRING, amount:DOUBLE>"
    json_options:
      rescuedDataColumn: _rescued_data
      readerCaseSensitive: true
```

Prefer `response.mode: raw` when the response must be treated as a full document per page, when you want explicit schema control, or when the payload is too large for direct in-memory materialization. In that mode, the connector downloads one row per page with the raw JSON in `raw_response`; structuring is handled by `shape.parse_json` with explicit Spark DDL.

```yaml
source:
  type: connector
  connector: rest_api
  name: nasa_eonet_events
  request:
    url: https://eonet.gsfc.nasa.gov/api/v3/events
    params:
      status: open
      limit: "50"
    headers:
      Accept: application/json
  response:
    mode: raw
    raw_column: raw_response
  limits:
    timeout_seconds: 60
    retry_attempts: 3
    max_page_bytes: 10485760
    max_total_bytes: 52428800

shape:
  parse_json:
    - column: raw_response
      alias: payload
      schema: >
        STRUCT<
          events: ARRAY<STRUCT<
            id: STRING,
            title: STRING,
            categories: ARRAY<STRUCT<id: STRING, title: STRING>>,
            geometry: ARRAY<STRUCT<date: STRING, type: STRING, coordinates: ARRAY<DOUBLE>>>
          >>
        >
```

Use `response.mode: records` when the API returns a list in `records_path` and each item represents a business row. Use `response.mode: raw` when the response should be handled by `shape` as a complete document. For large payloads or recurring replay, land files in storage and process them with Auto Loader.
