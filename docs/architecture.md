# Architecture

This document is for contributors and maintainers. It explains how ContractForge is structured, where behavior lives, which contracts must remain stable and how new features should be added without turning the framework into a collection of case-specific workarounds.

For user-facing usage, see [Usage Guide](usage-guide.md). For product documentation, use the website:

https://marquesantero.github.io/contractforge/

## Product Boundary

ContractForge is a Python framework for contract-first ingestion on Spark and Delta Lake. It runs inside notebooks, Databricks jobs, Databricks Asset Bundles or regular Python code with PySpark.

It provides:

- Declarative ingestion contracts.
- Source connector resolution.
- Declarative transformations for JSON, structs and arrays.
- Delta write modes with explicit semantics.
- Schema policy, typed watermarks and quality gates.
- Control tables for operational evidence.
- Governance metadata for annotations, operations and access.
- Optional extension points for presets, connectors, quality rules and write modes.

It intentionally does not provide:

- A scheduler or DAG engine.
- A managed runtime like Delta Live Tables/Lakeflow.
- A universal data quality catalog.
- A credential authority.
- A UI application.

The design goal is to keep ingestion behavior deterministic and auditable while remaining usable from normal Spark jobs.

## Repository Layout

```text
src/contractforge/
  __init__.py              Public exports and package version.
  _spark.py                Lazy SparkSession resolution and runtime detection.
  _sql.py                  SQL identifier and literal helpers.
  _uc_capabilities.py      Unity Catalog feature detection helpers.
  bundles.py               High-level bundle execution helpers.
  cli.py                   contractforge CLI commands.
  config.py                Global config, literals, constants and defaults.
  contract_bundle.py       Split contract loading and governance previews.
  contract_schema.py       JSON Schema generation and static shape validation.
  cost.py                  Operational cost estimation from control tables.
  exceptions.py            Public exception types.
  execution.py             Execution windows and catchup helpers.
  governance.py            Annotations, operations and access contracts.
  hooks.py                 Controlled pre/post ingestion hooks.
  ingestion.py             Batch ingestion orchestrator.
  lineage.py               Explain plan and OpenLineage event persistence.
  maintenance.py           Control-table retention and maintenance commands.
  plan.py                  IngestionPlan, SourceSpec, ConnectorSpec and normalization.
  presets.py               Preset registry and expansion.
  quality.py               Quality gate evaluation and custom rule registry.
  schema.py                Hashing, deduplication, encoding and schema evolution.
  shape.py                 Declarative JSON/struct/array transformations.
  sources.py               Connector registry and source resolution.
  state.py                 Control table DDL, logging, locks and retries.
  streaming.py             Auto Loader available-now orchestration.
  templates.py             Official contract templates and recommendations.
  watermark.py             Watermark encoding, filtering and computation.
  writers.py               Delta write modes and write-mode registry.
```

Tests are split into pure tests and Spark integration tests. Pure tests must not require a Spark session. Spark tests should skip cleanly when Java/Spark is unavailable.

## Public API

The supported public API is exported from `contractforge.__init__`. Internal modules may change between minor versions unless an object is explicitly exported.

Important public entry points:

- `ingest(**kwargs)`
- `ingest_plan(plan)`
- `ingest_stream_plan(plan)`
- `ingest_bundle(bundle)`
- `load_contract_bundle(path)`
- `validate_plan_shape(contract)`
- `yaml_schema()`
- `list_presets()`, `get_preset()`, `register_preset()`
- `list_source_resolvers()`, `register_source_resolver()`
- `register_write_mode()`
- `register_quality_rule()`
- `apply_ctrl_retention()`, `build_ctrl_retention_plan()`
- `analyze_operational_cost()`

User-facing failures should raise clear exceptions or return redacted failure payloads. Failed ingestion APIs raise `ContractForgeExecutionError` by default after control-table persistence.

## Contract Model

`IngestionPlan` is the normalized runtime contract. Users may create it directly or indirectly through `ingest(**kwargs)` and YAML dictionaries.

Core contract groups:

- Identity: source, target, catalog, layer, schema, table.
- Source: `SourceSpec` or `ConnectorSpec`.
- Preparation: select, mapping, shape, filters, custom keys and deduplication.
- Incremental state: watermarks, windows and catchup.
- Write semantics: mode, keys, partitions, clustering and Delta properties.
- Schema and quality: schema policy, type widening, rules and failure action.
- Operations: idempotency, lock, retry, cache and raise behavior.
- Governance: annotations, operations and access.
- Observability: explain mode, OpenLineage and runtime metadata.

Normalization lives in `plan.py`. Keep validation near normalization when the rule is static and independent from Spark data. Runtime validation that needs a DataFrame schema or target table state belongs in `ingestion.py`, `schema.py` or the specific connector/writer module.

## Contract Bundles

Split contracts model ownership boundaries:

- `*.ingestion.yaml`: execution contract.
- `*.annotations.yaml`: catalog comments, tags, aliases and PII metadata.
- `*.operations.yaml`: ownership, criticality, frequency, SLA and runbook.
- `*.access.yaml`: grants, row filters and column masks.

`contract_bundle.py` loads these files into a `ContractBundle`. `bundles.py` provides higher-level execution helpers. Access application is intentionally deferred because it often requires different permissions from ingestion.

## Execution Flow

### Batch `ingest_plan`

The batch orchestrator follows a deterministic flow:

1. Resolve and validate the plan.
2. Ensure control tables.
3. Register or evaluate idempotency.
4. Acquire cooperative lock when enabled.
5. Resolve source into a DataFrame.
6. Apply preparation:
   - `select_columns`
   - `column_mapping`
   - `transform.shape`
   - `filter_expression`
   - custom keys
   - watermark filter
   - deduplication
   - optional encoding fix
   - technical column protection
7. Validate schema and mode-specific constraints.
8. Evaluate quality gates.
9. Return dry-run payload without side effects when `dry_run=True`.
10. Execute the write mode.
11. Compute and persist watermark/state.
12. Persist run result and metrics.
13. Persist errors, explain and lineage when configured.
14. Release lock in `finally`.
15. Raise `ContractForgeExecutionError` on failed final status unless disabled.

Control-table writes are intentionally separate from the target Delta transaction. A target write can succeed and a later governance or logging step can fail; the run status must make that visible.

### Streaming / Auto Loader

`streaming.py` handles finite Auto Loader executions using `available_now`.

The stream parent run coordinates:

- Source stream resolution.
- Checkpoint location.
- `foreachBatch` child executions.
- Aggregated metrics in `ctrl_ingestion_streams`.

Each micro-batch calls `ingest_plan()` with a DataFrame source and a `parent_run_id`. Aggregated stream metrics must be derived from child results or, as a fallback, from child rows in `ctrl_ingestion_runs`.

Continuous streaming is outside the current product boundary.

## Source Resolution

`sources.py` owns connector registration and source resolution.

Built-in connector families:

- Table and SQL.
- Spark file formats.
- HTTP file.
- Object storage: S3, ADLS/Azure Blob, GCS and generic object storage.
- JDBC and named database connectors.
- REST API.
- Auto Loader.
- External Spark connectors such as Snowflake and BigQuery.

Connector principles:

- Connectors read data and return a DataFrame plus metadata.
- Connectors should not encode business transformations.
- Secrets must be resolved through placeholders and redacted before persistence.
- Runtime-specific setup must be explicit and documented.
- Serverless object storage should prefer external locations when direct credentials are not supported.
- Connector behavior must stay general; do not specialize for a single example dataset.

If a real ingestion test reveals a missing behavior, prefer a general connector capability over a workaround in an example notebook.

## Transformations

`shape.py` provides declarative structural transformations after source resolution:

- Parse JSON string columns with explicit Spark DDL.
- Flatten structs.
- Select nested paths with aliases.
- Extract array items by index.
- Explode arrays and arrays of structs.
- Zip parallel arrays.
- Deduplicate rows with deterministic SQL ordering.

Transformations belong to `transform.shape` because they change the DataFrame shape. Table and field descriptions, PII and tags belong to annotations. The two concepts should remain separate:

- Transform: how data is structurally prepared.
- Annotation: how the resulting table and columns are described.

Bronze cardinality changes are guarded because exploding raw payloads too early can create irreversible semantics. Silver is usually the better place for normalization.

## Schema Handling

`schema.py` contains:

- Row hash computation.
- Deduplication by SQL order expression.
- Custom key construction.
- Encoding fixes.
- Schema comparison.
- Additive schema synchronization.
- Optional type widening.

Schema policies:

- `permissive`: allows broad source movement.
- `additive_only`: allows new columns, blocks removals and unsafe type changes.
- `strict`: requires alignment.

Schema evolution must be auditable. Changes are recorded in control tables; automatic destructive changes are not performed.

## Quality Gates

`quality.py` evaluates built-in and custom rules. It favors consolidated aggregations to avoid one Spark action per rule.

Rule categories:

- Row-isolating rules: can quarantine bad rows.
- Dataset-level rules: must abort because no single row can be isolated.

Abort-only examples:

- `required_columns`
- `unique_key`
- `min_rows`

Custom quality rules should return explicit metadata and avoid hidden Spark actions when possible.

## Write Modes

`writers.py` implements official write modes:

- `scd0_append`
- `scd0_overwrite`
- `scd1_upsert`
- `scd1_hash_diff`
- `scd2_historical`
- `snapshot_soft_delete`

Write-mode principles:

- The mode name must describe the business semantics.
- Required keys must be validated before writing.
- Metrics should be logical first and Delta-history backed when available.
- Serverless/Spark Connect compatibility matters; prefer SQL where Python Delta APIs are unsupported.
- Write modes should be registered through the registry instead of extending large `if/elif` dispatch chains.

### Snapshot Soft Delete

`snapshot_soft_delete` is a complete-source mode. It inserts new rows, updates changed/reactivated rows and marks missing keys as inactive.

It must reject:

- `watermark_columns`
- `filter_expression`
- any source configuration that does not represent a complete snapshot

The implementation uses SQL `MERGE` to keep behavior consistent across Databricks classic and serverless runtimes.

## Control Tables

`state.py` owns DDL and operational persistence. Control tables are the evidence layer for ingestion.

Main tables:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_state`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_locks`
- `ctrl_ingestion_explain`
- `ctrl_ingestion_lineage`
- `ctrl_ingestion_metadata`
- `ctrl_ingestion_schema_changes`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_annotations`
- `ctrl_ingestion_operations`
- `ctrl_ingestion_access`

Control table schema changes must be additive. Existing columns are not removed automatically.

## Governance

`governance.py` owns annotations, operations and access contracts.

Annotations:

- Table comments.
- Column comments.
- Table and column tags.
- Aliases.
- PII metadata.
- Deprecation metadata where applicable.

Operations:

- Business owner.
- Technical owner.
- Support and escalation group.
- Criticality.
- Frequency.
- Freshness SLA.
- Runbook URL.

Access:

- Grants.
- Row filters.
- Column masks.
- Drift reports.

Unity Catalog capabilities vary by runtime and workspace. `_uc_capabilities.py` detects support and lets policies decide whether to warn or fail.

## Observability

Every run result should be useful without requiring the user to parse logs. Important fields include:

- status
- run identifiers
- target and source metadata
- framework and control schema version
- runtime type, Spark version and Python version
- rows read/written/inserted/updated/deleted/quarantined
- quality status
- watermark previous/current
- stage durations
- Delta versions
- error message
- governance result

Structured control tables are the primary integration point for dashboards and alerts.

## Security and Redaction

Secrets are represented as placeholders:

```text
{{ secret:scope/key }}
```

Resolution order:

1. Environment variable `CONTRACTFORGE_SECRET_<SCOPE>_<KEY>`.
2. Databricks Secrets through `dbutils.secrets.get`.

Redaction must happen before writing to:

- logs
- returned payloads
- control tables
- lineage events
- explain records
- error traces

When adding connector metadata, assume anything derived from connection options may contain credentials and must pass through redaction helpers.

## Extension Points

Supported extension points:

- Source resolver registry.
- Write-mode registry.
- Quality-rule registry.
- Preset registry.
- Controlled ingestion hooks.

Guidelines:

- Extension APIs should be small and documented.
- Extensions should receive normalized contracts, not raw YAML.
- Extensions should return explicit metadata.
- Extensions must not bypass redaction or control-table recording.

## CLI

The CLI is for local validation, schema generation, template discovery and operational maintenance.

Important commands:

- `contractforge validate`
- `contractforge schema`
- `contractforge templates list`
- `contractforge templates show`
- `contractforge templates write`
- `contractforge templates wizard`
- `contractforge maintenance ctrl-retention`
- `contractforge maintenance cost-report`
- connector diagnostics commands where available

CLI output is part of the user experience and should remain English, actionable and deterministic.

## Testing Strategy

Use three levels of tests:

1. Pure unit tests for normalization, schema generation, templates, redaction and helper behavior.
2. Local Spark tests for DataFrame transformations, quality gates and write modes.
3. Databricks real-ingestion harness tests for runtime compatibility, object storage, JDBC, Auto Loader and governance behavior.

When a real ingestion test finds a gap:

- First ask whether the behavior should be general.
- If yes, fix the library.
- If no, document it as an example-specific concern.
- Avoid adding dataset-specific special cases.

## Contribution Guidelines

Before changing behavior:

1. Identify the public contract affected.
2. Decide whether the change belongs in a connector, transform, writer, quality rule, schema policy or governance module.
3. Add validation close to the normalization/runtime boundary that owns the rule.
4. Add tests at the lowest level that proves the behavior.
5. Update user docs and templates when the behavior is user-facing.
6. Record architectural trade-offs as ADRs when the decision affects future design.

## Design Principles

- Contracts are the product boundary.
- Runtime behavior must be observable.
- Defaults should be safe, not surprising.
- SQL is preferred when it improves cross-runtime compatibility.
- Source connectors read; transformations shape; annotations describe.
- Control tables are append/evolve, not hidden logs.
- Errors should be explicit and actionable.
- Features should be general enough to serve more than one example.
- Documentation should explain how a user succeeds, not just list parameters.
