# Control Tables Dashboard

This directory documents a Databricks SQL / Lakeview dashboard for operating ContractForge pipelines. It intentionally provides SQL, layout guidance and visual structure instead of a Lakeview JSON export, because Databricks dashboard internals may change between runtime versions.

## Files

- `control_tables_dashboard.sql`: named queries for KPI cards, charts, drill-down tables and quality views.
- `control_tables_dashboard_blueprint.yaml`: recommended page hierarchy, filters, widgets and visualization types.

## Goal

The dashboard should behave like an operations command center, not like a loose collection of SQL snippets. The first page should quickly answer:

- Is the ingestion environment healthy?
- Which targets failed or breached SLA?
- How much data was read, written and quarantined?
- Which stage is the bottleneck: read, quality, write, lineage or state update?
- Which connectors and runtimes are producing most failures?

## Required Placeholders

Before saving the queries in Databricks SQL, replace these placeholders:

| Placeholder | Example | Meaning |
| --- | --- | --- |
| `{{catalog}}` | `main` | Catalog that stores the control tables. |
| `{{ctrl_schema}}` | `ops` | Schema that stores the control tables. |
| `{{lookback_days}}` | `7` | Default historical window for charts. |

Databricks SQL query parameters do not parameterize identifiers such as catalog and schema names. Replace them in the SQL text before publishing the dashboard.

## Recommended Dashboard Pages

Create a dashboard named **ContractForge Operations Command Center**.

| Page | Purpose | Main views |
| --- | --- | --- |
| Overview | Executive health of the environment. | KPI cards, status trend, recent failures. |
| Reliability | Target health and SLA. | Target/status matrix, freshness, runbooks. |
| Performance | Runtime and throughput analysis. | Duration, rows per second, stage bottlenecks. |
| Quality | Quality governance. | Rule failures, severities, quarantine and effective rows. |
| Streaming | Auto Loader and foreachBatch evidence. | Parent streams, micro-batches and parent/child reconciliation. |
| Connectors & Governance | Source and governance adoption. | Connectors, runtimes, annotations and operations coverage. |

## Global Filters

Use these filters when the dashboard tool allows it:

- `run_date`
- `layer`
- `status`
- `target_table`
- `source_connector`
- `source_provider`
- `runtime_type`
- `criticality`

For executive pages, use a 7 or 14 day lookback. For troubleshooting, use 30 or 90 days.

## Visual Standards

Use consistent colors:

| Status | Suggested color |
| --- | --- |
| `SUCCESS` | Green |
| `FAILED` | Red |
| `SKIPPED` | Gray |
| `DRY_RUN` | Blue |
| `WARN` / `WARNING` | Yellow |
| `BREACHED` | Red |
| `NO_SUCCESS` | Orange |

Avoid pages with many tiny charts. Prefer a small number of readable charts with drill-down tables below them.

## Publishing Steps

1. Open `control_tables_dashboard.sql`.
2. Replace `{{catalog}}`, `{{ctrl_schema}}` and `{{lookback_days}}`.
3. Create one query per named SQL block.
4. Build pages following `control_tables_dashboard_blueprint.yaml`.
5. Validate first with a short period, for example 7 days.
6. Share the dashboard only with groups allowed to read the control tables.

## Permissions

The dashboard requires `SELECT` on the control tables used by its queries:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_operations`

If governance is enabled, these tables are also useful:

- `ctrl_ingestion_annotations`
- `ctrl_ingestion_access`
- `ctrl_ingestion_schema_changes`

## Security Notes

- Do not grant broad access to `ctrl_ingestion_quarantine` without reviewing sensitive-data policy. The `record_payload` field can contain rejected source records.
- Do not enable `explain_mode=True` in continuous production runs just to feed the dashboard. Explain records are diagnostic evidence, not routine metrics.
- Do not treat `FAILED = 0` as the only health signal. Review SLA, quarantine, row-volume drops and stream reconciliation.

## Useful Operating Questions

Use the dashboard to answer:

- Which targets have no successful run in the last expected interval?
- Which runs are repeatedly skipped by idempotency?
- Which connector has the highest failure rate?
- Which tables have increasing quarantine volume?
- Which stage dominates runtime?
- Are streaming parent runs consistent with their child batch runs?
- Are high-criticality tables missing operations metadata?
