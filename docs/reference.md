# Reference Documentation

The official user documentation is the website:

https://marquesantero.github.io/contractforge/

This Markdown file exists to keep the repository documentation map stable and to record content that should be kept aligned with the website. It should not become a second full manual competing with the site.

## Source of Truth

Use these locations as source of truth:

| Topic | Primary location |
| --- | --- |
| Product overview and onboarding | Website home and [README](../README.md) |
| First ingestion | [Quick start](quickstart.md) and website quick start |
| Daily usage | [Usage guide](usage-guide.md) |
| Contributor internals | [Architecture](architecture.md) |
| Contract examples | [Templates](templates.md) and `examples/` |
| Connector details | Website connector pages and [Connector compatibility](connector-compatibility.md) |
| Operational maintenance | [Operations](operations.md) |
| Security | [Security](security.md) |
| ADRs | [ADRs](adrs/README.md) |

## Content To Keep Reflected On The Website

The previous monolithic Markdown reference contained useful content that should be preserved on the website as structured pages instead of being maintained here as a duplicate manual.

### Concepts

- Contract-first positioning.
- What ContractForge does and does not do.
- Logical `layer` versus physical `target.schema`.
- Execution order.
- Control-table evidence model.
- Runtime compatibility: classic, serverless/Spark Connect and local Spark.

### Installation and Runtime Setup

- PyPI installation.
- Databricks wheel installation.
- Local development setup.
- Optional extras such as `spark` and `aws`.
- Databricks Serverless limitations and external-location guidance.
- Classic cluster guidance for direct credential-based object storage access.

### Public API

- `ingest`.
- `ingest_plan`.
- `ingest_stream_plan`.
- `ingest_bundle`.
- `load_contract_bundle`.
- `validate_plan_shape`.
- `yaml_schema`.
- Template, preset, source, write-mode and quality-rule registries.
- `ContractForgeExecutionError` and `raise_on_failure`.

### Ingestion Contract

Document all contract groups with examples:

- Source and target identity.
- Logical layer and physical schema.
- Source connectors.
- Column selection and mapping.
- `transform.shape`.
- Filters and watermarks.
- Merge/hash keys.
- Deduplication.
- Delta layout and properties.
- Schema policy and type widening.
- Quality rules.
- SCD2 configuration.
- Snapshot soft delete constraints.
- Idempotency, retry and locks.
- OpenLineage and explain mode.
- Runtime parameters and operational lineage.

### Connectors

Each connector page should contain:

- What it reads.
- Required fields.
- Optional read/options/auth fields.
- YAML and Python examples.
- Supported runtime notes.
- Credential handling.
- Pushdown or partitioning behavior when applicable.
- Common failure modes.

Connector pages to keep complete:

- Table and SQL.
- Files.
- HTTP file.
- REST API.
- JDBC.
- S3.
- Azure Blob / ADLS.
- Object storage generic.
- Auto Loader.
- External Spark connectors such as Snowflake and BigQuery.

### Transformations

Keep examples for:

- JSON string parsing with explicit DDL.
- Struct flattening.
- Nested path extraction.
- Array item extraction.
- Explode and explode_outer.
- Arrays of structs.
- Parallel arrays with `zip_arrays`.
- Deduplicate by deterministic SQL order.
- Bronze cardinality guardrails.

### Write Modes

Each write mode should document:

- Business semantics.
- Required fields.
- Optional fields.
- Metrics emitted.
- Runtime compatibility.
- Common mistakes.
- YAML and Python examples.

Write modes:

- `scd0_append`
- `scd0_overwrite`
- `scd1_upsert`
- `scd1_hash_diff`
- `scd2_historical`
- `snapshot_soft_delete`

### Quality, Schema and Watermarks

Keep detailed pages for:

- Built-in rules.
- `QualityExpression`.
- Custom quality-rule registry.
- Abort-only rules.
- Quarantine behavior.
- Schema policies.
- Type widening.
- Schema change audit.
- Simple and composite watermarks.
- Backfill and catchup windows.

### Governance

Keep separate pages for:

- `ingestion.yaml`
- `annotations.yaml`
- `operations.yaml`
- `access.yaml`
- Bundle loading.
- Governance preview/check.
- Annotation application.
- Access drift handling.
- Unity Catalog capability detection.

### Operations

Keep operational pages for:

- Control table catalogue.
- Control table field reference.
- Run troubleshooting.
- Retention and vacuum.
- Operational dashboard.
- Cost analyzer.
- SLA/freshness queries.
- Streaming parent/child reconciliation.

### Examples and Templates

Keep examples aligned with validated real-ingestion scenarios:

- HTTP CSV snapshot.
- Object-storage nested JSON with shape.
- Small files folder ingestion.
- Auto Loader available-now.
- JDBC/RDS IAM hash diff.
- Raw JSON string payload parsing.
- Parallel array shaping.

## Maintenance Rule

When a new user-facing feature lands:

1. Update the website page for the feature.
2. Update a smaller Markdown guide only if it is the right long-lived source for the topic.
3. Update this file only if a new documentation area must be tracked.
4. Avoid reintroducing a monolithic Markdown manual.
