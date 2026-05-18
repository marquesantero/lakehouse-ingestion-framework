# Changelog

This project follows semantic versioning while the library evolves:

- `PATCH`: bug fix without contract changes.
- `MINOR`: compatible feature or planned contract hardening.
- `MAJOR`: incompatible change after stable adoption.

## Unreleased

- Makes ingestion APIs fail fast for callers by default: `ingest()`, `ingest_plan()`, `ingest_stream_plan()` and `ingest_bundle()` now raise `ContractForgeExecutionError` when the final result status is `FAILED` or `ABORTED`.
- Adds `raise_on_failure=False` as an explicit runtime option for tests, notebooks or orchestration code that need to inspect failed result payloads directly.
- Keeps control-table logging, error persistence and stream/execution aggregation before raising to the caller.

## 2.12.0 - 2026-05-17

- Adds official templates derived from validated real ingestions: HTTP CSV, object storage with nested JSON, small files, Auto Loader `available_now`, RDS/Aurora IAM hash diff, JSON payload string and parallel arrays.
- Updates the `templates wizard` indirectly through metadata so it can recommend real patterns by `source`, `mode` and `pattern`.
- Documents the new templates and reinforces `transform.shape` and `transform.deduplicate` in generated bundles.

## 2.11.0 - 2026-05-17

- Adds `contractforge.cost` with `CostModel`, `build_operational_cost_query`, `operational_cost_dataframe` and `analyze_operational_cost`.
- Adds `contractforge maintenance cost-report` to estimate operational cost/efficiency from `ctrl_ingestion_runs`.
- Documents throughput, stage duration and estimated cost per million rows, explicitly stating that it is not provider billing.

## 2.10.0 - 2026-05-17

- Adds `execution.window` to run backfills as temporal sub-runs with `parent_run_id`, `[start, end)` filters and aggregated metrics in the parent result.
- Adds `execution.catchup` to generate windows from the saved watermark when `start` is omitted.
- Documents YAML examples for backfill/catchup, idempotency per window and historical watermark restrictions.

## 2.9.0 - 2026-05-17

- Adds `contractforge templates wizard` to recommend templates by `layer`, `source`, `mode` and `pattern`, with an option to write the best YAML bundle.
- Adds `bronze_blob_partitioned_files` and `silver_scd1_hash_diff` templates covering partitioned object storage and hash diff.
- Exposes `recommend_contract_templates()` as a public API for notebooks, automation and DX tools.

## 2.8.0 - 2026-05-17

- Adds `source.read.file_regex` to filter files by regex before `spark.read`, without replacing `pathGlobFilter`.
- Supports `source.read.file_regex_scope` with `filename` or `relative_path`, explicit limit through `source.read.file_regex_max_listed`, and recursion controlled by `source.read.file_regex_recursive` or `recursiveFileLookup`.
- Records listing and match metrics in `source_metrics_json`, including `files_listed`, `files_matched` and `file_regex_applied`.

## 2.7.0 - 2026-05-17

- Adds `transform` as the canonical namespace for physical pre-quality/write transformations.
- Adds `transform.shape` as the recommended form for JSON parsing, flattening, arrays and structural projection, while keeping `shape` as a validated shortcut.
- Adds `transform.deduplicate` with explicit `keys` and `order_by` to deduplicate batches before MERGE/quality/write.
- Updates JSON Schema, documentation and tests for the new declarative transformation contract.

## 2.6.9 - 2026-05-17

- Blocks `scd1_upsert`, `scd2_historical` and `snapshot_soft_delete` when the source contains multiple rows for the same `merge_keys`, preventing ambiguous MERGE before writing.
- Reuses an already-approved `quality_rules.unique_key` check to avoid an extra pass when the rule is equivalent to `merge_keys`.
- Redacts tracebacks and error messages before logging or persisting to control tables, reducing the risk of secret leakage in connector/runtime errors.

## 2.6.8 - 2026-05-17

- Improves short `error_message` values for Spark/JVM exceptions by prioritizing useful lines such as `StorageException`, `AnalysisException`, `PSQLException`, `ValueError` and `Caused by`, instead of generic frames like `java.lang.Thread.run`.
- Keeps full traceback in `ctrl_ingestion_errors`; this change only affects the short summary in `ctrl_ingestion_runs` and the result.
- Allows simple indexes in REST/HTTP JSON `response.records_path`, such as `$[1]` and `$.data[0].items`, without turning the feature into full JSONPath.

## 2.6.7 - 2026-05-17

- Accepts `source.schema` as an alias for `source.read.schema` in declarative connectors, preventing explicit schemas from being silently ignored.
- Rejects conflicts between `source.schema`, `source.read.schema` and `source.options.schema` with a clear error before reading.
- Updates the contract JSON Schema to recognize `source.schema`.

## 2.6.6 - 2026-05-17

- Allows `source.auth.credential_provider=default_chain` for JDBC `auth.type=rds_iam`, using the AWS credential provider chain through `botocore` when explicit credentials/environment variables are not used.
- Records `jdbc_rds_iam_credential_source` for safe auditing of credential origin (`explicit`, `env` or `default_chain`) without exposing secrets.
- Keeps the existing secrets/environment path and adds clear errors for unsupported providers or runtimes without `botocore`.

## 2.6.5 - 2026-05-17

- Adds `source.auth` for JDBC connectors, separating credentials from `source.options`.
- Supports `auth.type=basic` for `user/password` and `auth.type=rds_iam` to generate Amazon RDS/Aurora IAM tokens in the Python driver without depending on `boto3` or AWS CLI.
- Records safe metrics such as `jdbc_auth_type`, `jdbc_rds_iam_token_generated` and RDS region, keeping username/password/token redacted in metadata.
- Documents correct JDBC connectivity alternatives for RDS: VPC peering/PrivateLink/same VPC, traditional public endpoint or Aurora Express Internet Access Gateway with IAM token.

## 2.6.4 - 2026-05-17

- Standardizes `rows_written` as the primary operational metric when the Delta runtime returns reliable counters even if the writer cannot calculate the count directly.
- Avoids reusing old `DESCRIBE HISTORY` metrics when the Delta version did not change, preserving `rows_written=0` for loads without a new commit.
- Fixes append/hash-diff observability in runtimes where `operationMetrics.numOutputRows` is the only reliable evidence of written rows.

## 2.6.3 - 2026-05-17

- Hardens `ensure_ctrl_tables` against concurrent initialization from multiple tasks using the same `ctrl_schema`.
- Stops updating `ctrl_ingestion_metadata` on every run when the current version is already registered.
- If another worker registers the same `framework_version`/`ctrl_schema_version` during a concurrent Delta conflict, the execution continues with a warning instead of failing before touching data.
- Keeps version auditing in `ctrl_ingestion_metadata` and adds regression tests for concurrent conflicts.

## 2.6.2 - 2026-05-17

- Adds declarative S3 credentials through `source.auth` in the `s3`/`object_storage` connector with `provider=s3`.
- Configures `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.session.token` and the appropriate credentials provider in classic/job cluster/local runtimes.
- Allows Hadoop S3A options in `source.options` with `fs.s3a.*` or `spark.hadoop.fs.s3a.*` keys, without passing them to the Spark reader.
- In serverless/Spark Connect, fails fast with guidance to use Unity Catalog External Location/Volume when `spark.conf.set` for `fs.s3a.*` is blocked.
- Keeps credentials redacted in metadata and adds safe metrics `s3_auth_configured`, `s3_temporary_credentials` and `s3_conf_options_configured`.

## 2.6.1 - 2026-05-16

- Fixes `rest_api` connector materialization for real JSON payloads with structs, arrays and heterogeneous optional fields.
- In classic Spark runtimes, `response.mode: records` now materializes records through JSON lines and `spark.read.json`, using RDD when available or configured `source.read.staging_path`, avoiding `createDataFrame` inference failures with complex arrays.
- Adds `source.read.schema` and `source.read.json_options` to explicitly control the Spark JSON reader used for REST/HTTP JSON record materialization, avoiding fragile inference in heterogeneous or dynamic-object payloads.
- Records `source_metrics.dataframe_materialization` to audit the path used by the REST connector.

## 2.6.0 - 2026-05-16

- Adds `source.read.schema` to declare Spark DDL schemas in file and object storage connectors.
- Avoids schema inference on many small files when the contract already knows the expected schema.
- Records `source_metrics.schema_declared` for connector observability.

## 2.5.2 - 2026-05-16

- Fixes `shape.columns` so all paths are projected from the original DataFrame schema.
- Avoids failure when an alias overwrites the name of a parent struct before extracting sibling fields, for example `amount._VALUE -> amount` and `amount._currency -> currency`.
- Adds a regression test for nested sibling field projection when an alias conflicts with the parent struct.

## 2.5.1 - 2026-05-16

- Fixes incremental connectors so typed watermark values are extracted before building predicates, parameters, headers or bodies.
- Fixes a real second-run JDBC incremental failure where the full watermark JSON was used as a SQL literal.
- Adds clear validation for composite watermarks in `source.incremental` when there is no single incremental column.

## 2.5.0 - 2026-05-16

- Makes `layer` a customizable logical classification instead of limiting contracts to `bronze`, `silver` and `gold`.
- Keeps `target_schema` as the explicit physical schema; when omitted, `layer` remains the physical schema fallback.
- Updates JSON Schema, `contractforge init`, tests and documentation to accept layers such as `stage`, `raw`, `trusted` and `curated`.
- Keeps Bronze operational restrictions only for the literal value `layer: bronze`.
- Adds retry when registering `ctrl_ingestion_metadata`, reducing control-table setup failures caused by concurrency.

## 2.4.3 - 2026-05-15

- Adds declarative Azure Blob with SAS support in the `azure_blob` connector.
- Allows `source.account_url`, `source.container` and `source.auth.sas_token`, automatically building the `wasbs://container@account.blob.core.windows.net/...` path.
- Configures `fs.azure.sas.<container>.<account>.blob.core.windows.net` at runtime when `sas_token` is provided.
- In Databricks serverless/Spark Connect, when `spark.conf.set` is blocked, fails fast with guidance for Unity Catalog External Location/Volume or Network Policy/NCC; there is no implicit REST fallback in `azure_blob`.
- Accepts `avro` and `xml` as file formats in file/object storage connectors, delegating reads to the Spark runtime.
- Keeps source secrets redacted in metadata and adds provider/container/auth metrics.

## 2.3.0 - 2026-05-15

- Adjusts `shape.columns` to act as declarative projection: when declared, only the listed aliases remain as business columns.
- Automatically removes ContractForge-managed technical columns inherited from the source before recreating them for the current run.
- Keeps the ability to preserve a source column with a reserved name through `column_mapping` to a non-reserved name.
- Improves Bronze -> Silver -> Gold composition without requiring `select_columns` only to clean technical metadata from the previous layer.
- Updates tests and documentation for `shape` and technical-column semantics.

## 2.2.0 - 2026-05-15

- Adds `response.mode: raw` to the `rest_api` connector to download complex JSON payloads as strings, one row per page.
- Keeps nested JSON structuring in `shape.parse_json`, with explicit DDL schema and no semantic transformation in the connector.
- Adds `response.raw_column` to name the raw payload column and `response_page_number` to trace pages.
- Adds `limits.max_page_bytes` and `limits.max_total_bytes` to protect the driver from large payloads.
- Records `response_mode`, `raw_payloads_read`, byte limits and bytes read in `source_metrics`.
- Updates REST API documentation with raw payload examples and the recommendation to use landing + Auto Loader for high volume.

## 2.1.0 - 2026-05-15

- Adds native `http_file` connector to download HTTP(S) files through the Python driver and materialize Spark DataFrames without depending on direct `spark.read` support for `https://`.
- Adds `http_csv`, `http_json` and `http_text` aliases.
- Supports `format=csv`, `json`, `jsonl`, `ndjson` and `text` in `http_file`.
- Adds static validation for `source.path`/`source.request.url`, `source.format` and HTTP GET method for HTTP file.
- Records specific metrics in `source_metrics_json`: format, records read, bytes downloaded, retry and `source_complete`.
- Updates connector documentation with an example of public CSV ingestion through HTTP.

## 2.0.0 - 2026-05-15

- **Breaking:** renames the Python namespace to `contractforge`; old imports through `lakehouse_ingestion` were removed.
- **Breaking:** updates internal observability/lineage references to the `contractforge` component.
- Keeps the distributed package and CLI as `contractforge`.
- Adds `shape.zip_arrays` to transform parallel arrays into `array<struct>` before `shape.arrays`.
- Allows modeling API responses such as Open-Meteo without manual `arrays_zip`/`explode` in notebooks.
- Extends `shape.columns` with `cast` and `expression` for simple structural normalizations.
- Automatically removes technical aliases from `zip_arrays`/`explode` when used only as bridges to final columns.
- Updates JSON Schema, public exports, tests and `shape` documentation.

## 1.16.0 - 2026-05-14

- Adds built-in contract templates for REST, Auto Loader, JDBC/SCD1, snapshot soft delete, SCD2 and gold KPI scenarios.
- Adds `contractforge templates list|show|write` CLI to discover and generate split YAML bundles.
- Exposes `list_contract_templates()`, `get_contract_template()`, `contract_template_details()` and `contract_template_files()` in the public API.
- Adds template documentation to accelerate onboarding and standardize new projects.

## 1.15.0 - 2026-05-14

- Adds `contractforge maintenance ctrl-retention` to generate or apply cleanup for historical control tables.
- Exposes `build_ctrl_retention_plan()` and `apply_ctrl_retention()` in the public API.
- Keeps `ctrl_ingestion_state` and `ctrl_ingestion_metadata` outside automatic cleanup.
- Strengthens connector metadata redaction, including labels, paths and tables with sensitive patterns.
- Adds audit tests to ensure REST/JDBC metadata does not expose credentials.
- Adds operational retention documentation, anti-patterns and JDBC/REST YAML examples.

## 1.14.0 - 2026-05-14

- Separates logical `layer` from the physical target schema with the new `target_schema` parameter.
- Keeps `layer` as the physical schema default when `target_schema` is not provided.
- Accepts contracts in `target: {catalog, schema, table}` format as a declarative alternative to `catalog`/`target_schema`/`target_table`.
- Updates `contractforge init --target-schema` to generate split bundles with annotations/operations/access pointing to the correct physical schema.
- Updates preview/governance, stream, ingestion and unqualified-source resolution to use the resolved physical schema.

## 1.13.0 - 2026-05-14

- Adds `contractforge init` to generate starter YAML contracts from the CLI.
- Supports single-contract generation or split bundle generation with `.ingestion.yaml`, `.annotations.yaml`, `.operations.yaml` and `.access.yaml`.
- Validates required keys for modes that need `merge_keys`/`hash_keys`.
- Updates documentation, site and project template with the `init -> validate-project` flow.

## 1.12.0 - 2026-05-14

- Adds `contractforge validate-project` to recursively discover and validate standalone contracts and split bundles in a project tree.
- Makes Databricks Asset Bundle CI easier without listing files one by one.
- Updates documentation and the project template with the new validation flow.

## 1.11.0 - 2026-05-14

- Adds `contractforge connectors doctor` to diagnose static connector requirements without opening a SparkSession or external connections.
- Exposes `diagnose_source_connectors()` in the public API.
- Documents runtime requirements for Auto Loader, object storage, JDBC, Snowflake and BigQuery.
- Updates README, official documentation, usage guide and site with the new command.

## 1.10.0 - 2026-05-14

- Adds native object-storage aliases: `s3`, `adls`, `azure_blob` and `gcs`, with inferred provider for observability.
- Adds `delta` and `orc` file connectors by path using `spark.read.format(...).load(path)`.
- Adds named JDBC aliases: `postgres`, `postgresql`, `sqlserver`, `mysql` and `oracle`.
- Adds external Spark connectors `snowflake` and `bigquery`, delegating to Spark connectors installed in the runtime.
- Strengthens static validation for the new connectors in `contractforge validate`.
- Updates README, official documentation and site with YAML examples for the new connectors.

## 1.9.0 - 2026-05-14

- Adds a declarative source connector layer with `ConnectorSpec` and `register_source_resolver` registry.
- Includes native connectors for table/view, SQL, files (`parquet`, `json`, `csv`, `text`), object storage/blob (`adls`, `azure_blob`, `s3`, `gcs`), JDBC and REST API.
- Allows custom connectors in YAML/JSON using any valid `source.connector` name when a resolver is registered at runtime.
- Adds `contractforge connectors list|show` CLI and static validation for required native connector fields in `contractforge validate`.
- Supports batch REST API with `bearer_token`, `api_key`, `basic` and `oauth_client_credentials` auth; `page`, `offset`, `cursor` and `link_header` pagination; retry/backoff, timeout and simple rate limiting.
- Supports incremental pushdown in REST (`watermark_param`, `watermark_header`, `watermark_body_field`) and JDBC (`watermark_column`/`predicate`) using the previous watermark recorded by the library.
- Supports Auto Loader through the unified `source.type=connector` and `connector=autoloader` format.
- Records redacted source metadata in `ctrl_ingestion_runs` (`source_connector`, provider, format, path, options, request/auth/pagination/incremental/limits and capabilities).
- Records connector-specific observability in `ctrl_ingestion_runs.source_metrics_json`, including requests/pages/bytes/records for REST and strategy/incrementality/partitioning for JDBC.
- Allows declaring `source.read.source_complete=true` or `full_snapshot=true` in connectors for modes that require a complete snapshot.
- Updates JSON Schema, public exports and documentation with YAML connector examples.
- Bumps `ctrl_schema_version` to 11.

## 1.8.1 - 2026-05-13

- Renames the product/distributed package to `contractforge`.
- Renames the CLI to `contractforge`.
- Moves `pyspark` and `delta-spark` to the optional `spark` extra, preventing wheels installed in Databricks/serverless from resolving dependencies already provided by the runtime.
- Keeps the `dev` extra with Spark/Delta for complete local tests and CI.

## 1.8.0 - 2026-05-13

- Adds `shape` to transform JSON/struct/array structures before quality/write.
- Supports recursive struct flattening, nested path extraction with aliases and arrays in `keep`, `to_json`, `size`, `first`, `explode` and `explode_outer` modes.
- Blocks cardinality changes in Bronze by default, requiring `shape.allow_cardinality_change_on_bronze=true`.
- Detects multiple sibling explodes that could create a Cartesian product, requiring `allow_cartesian=true`.
- Adds YAML examples for flattening and nested arrays in the documentation.

## 1.7.0 - 2026-05-13

- Adds declarative presets for common Bronze/Silver/Gold ingestion patterns.
- Exposes `apply_preset`, `list_presets`, `get_preset`, `preset_details` and `register_preset`.
- Adds `contractforge presets list|show` and `validate --expand-presets` CLI commands.
- Records `applied_presets` in the plan and execution result for auditability.
- Adds reusable quality, Delta properties, runtime and governance modifiers.

## 1.6.4 - 2026-05-13

- Defines explicit semantics for `access_policy.on_drift`.
- `on_drift=fail` now fails before applying grants when drift exists.
- `validate-access` and `governance-check` return `FAILED` for drift with `on_drift=fail` and `WARNED` for tolerated drift.
- Drift issues now reflect `fail` or `warn` severity according to policy.

## 1.6.3 - 2026-05-13

- Makes `governance-apply` apply only `operations` and `annotations`, keeping `access` exclusive to the dedicated command.
- Adds `apply_annotations_bundle()` API and `contractforge apply-annotations` CLI.
- Adds `contractforge validate-access` CLI to validate access contracts and drift without applying changes.
- Adds structured `annotations_preview` to `dry_run` results.

## 1.6.2 - 2026-05-13

- Separates access lifecycle from normal ingestion: `ingest_plan` applies `operations`/`annotations` and leaves `access` as `DEFERRED`.
- Adds `apply_access_bundle()` API and `contractforge apply-access` CLI.
- Adds `contractforge drift-check` CLI alias.
- Adds conservative Unity Catalog capability validation for tags, row filters and column masks.

## 1.6.1 - 2026-05-13

- Accepts the split format with `target`, `operations`/`ownership`, `access_policy` and `column_masks` as a per-column map.
- Blocks `revoke_unmanaged=true` without explicit confirmation in the dedicated access command.
- Strengthens governance validation: `expected_frequency`, UC privileges, qualified functions, empty descriptions, empty aliases and incomplete `deprecated`.
- Expands auditing in `ctrl_ingestion_annotations`, `ctrl_ingestion_operations`, `ctrl_ingestion_access` and the governance summary in `ctrl_ingestion_runs`.
- Bumps `ctrl_schema_version` to 9.

## 1.6.0 - 2026-05-13

- Adds split declarative contracts for `annotations`, `operations` and `access`.
- Applies table and column comments/tags, including aliases, PII and deprecation, with auditing in `ctrl_ingestion_annotations`.
- Records the operations contract in `ctrl_ingestion_operations` for dashboards and external alerts.
- Applies declarative grants, row filters and column masks with auditing in `ctrl_ingestion_access`.
- Adds bundle loader (`load_contract_bundle`) and `contractforge validate-bundle` CLI.
- Adds `_metadata` per contract file and governance preview (`governance_preview`).
- Adds asynchronous governance application (`apply_governance_bundle`) and `governance-preview`/`governance-apply` CLI.
- Adds governance validation against the real target schema (`validate_governance_contract`) and `governance-check` CLI.
- Adds grants drift report (`access_drift_report`), `previous_value` population and `revoke_unmanaged=true` support.
- Bumps `ctrl_schema_version` to 8.

## 1.5.1 - 2026-05-13

- Fixes aggregated metrics for Autoloader/`SourceSpec` streams when `foreachBatch` registers child batches but local driver state does not reflect results in Spark Connect/serverless.
- Normalizes micro-batch metrics between `rows_*` and `total_rows_*` before consolidating `ctrl_ingestion_streams`.

## 1.5.0 - 2026-05-12

- Adds declarative `SourceSpec` for Autoloader in `available_now` mode.
- Adds source resolver registry (`register_source_resolver`).
- Adds `ingest_stream_plan` with `foreachBatch` reusing `ingest_plan` per batch.
- Adds `ctrl_ingestion_streams` and bumps `ctrl_schema_version` to 7.
- Applies idempotency at stream level and per batch to avoid duplicate reruns.

## 1.4.0 - 2026-05-12

- Adds `column_mapping` to rename source -> target with validation for collisions and technical columns.
- Adds `delta_properties` to apply TBLPROPERTIES when creating Delta tables.
- Allows per-plan `retry_attempts` and `retry_backoff_seconds`.
- Blocks silent overwrite of technical columns from the source.
- Validates fully null `merge_keys` before executing `MERGE` and warns for partial nulls.
- Optimizes `quality_rules.expressions` into the single-pass quality aggregation.
- Adds `IngestionHooks`, `register_write_mode`, `yaml_schema()` and `contractforge validate/schema` CLI.

## 1.3.1 - 2026-05-11

- Adds CI workflow for lint, pure tests and build validation.
- Adds `scripts/check_release.py` to ensure version, changelog and package metadata are synchronized.
- Exposes project URLs in wheel metadata.

## 1.3.0 - 2026-05-11

- Hardens `quality_rules` parsing by rejecting unknown fields, invalid thresholds and malformed expressions.
- Normalizes declarative lists in quality rules from YAML/notebooks.
- Validates `runtime_parameters`, `tags`, `idempotency_policy` and `allow_type_widening`/`schema_policy` combinations early.
- Updates package metadata to SPDX license format and adds `LICENSE`.

## 1.2.0 - 2026-05-11

- Adds `allow_type_widening` for safe type widening.
- Records structural evolution in `ctrl_ingestion_schema_changes`.
- Exposes `stage_durations` and `contract_metadata` in ingestion results.
- Propagates declarative metadata to `ctrl_ingestion_runs`.

## 1.1.0 - 2026-05-11

- Formalizes severity and message in `quality_rules.expressions`.
- Standardizes error, runtime and logical metrics observability.
- Evolves operational idempotency and best-effort locks.

## 1.0.2 - 2026-05-11

- Fixes deduplication compatibility with Spark Connect/serverless.
- Adjusts SCD hash diff and SCD2 to avoid ambiguous references after joins.

## 1.0.0 - 2026-05-11

- First functional line validated with Databricks harness and Medallion flow.
