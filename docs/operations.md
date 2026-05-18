# Operations and Maintenance

This guide covers operational routines that are not part of ingestion itself but keep the environment healthy.

## Control Table Retention

`ctrl_*` tables are operational evidence. Keep them long enough for audit, support and troubleshooting, but do not let them grow indefinitely.

Initial recommendation:

| Environment | Suggested retention | Notes |
| --- | --- | --- |
| Development | 15 to 30 days | Avoids local accumulation. |
| Staging | 30 to 90 days | Useful for regressions and release validation. |
| Production | 180 to 400 days | Adjust to audit, privacy and internal policies. |

Current state (`ctrl_ingestion_state`) and version metadata (`ctrl_ingestion_metadata`) are not part of historical cleanup. They represent current operational state.

## Preview

By default, the command only prints the SQL plan:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180
```

Clean only selected tables:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 90 \
  --target runs \
  --target errors \
  --target quarantine
```

## Apply Cleanup

Use `--apply` only from a controlled operational job:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180 \
  --apply
```

Run `VACUUM` after the `DELETE`s when your Delta retention policy allows it:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180 \
  --vacuum \
  --vacuum-retention-hours 168 \
  --apply
```

## Recommended Operations Contract

In projects with split contracts, record the operational criticality of each ingested table in `*.operations.yaml`:

```yaml
target:
  catalog: main
  schema: sales_curated
  table: s_orders

ownership:
  business_owner: sales-ops
  technical_owner: data-platform
  support_group: data-platform

operations:
  criticality: high
  expected_frequency: daily
  freshness_sla_minutes: 180
  alert_on_failure: true
  alert_on_quality_fail: true
  runbook_url: https://wiki.example.com/runbooks/s_orders
  tags:
    maintenance_window: "02:00-04:00 UTC"
```

ContractForge does not send alerts directly. It records enough data in `ctrl_ingestion_runs`, `ctrl_ingestion_errors`, `ctrl_ingestion_quality`, `ctrl_ingestion_streams` and `ctrl_ingestion_operations` for dashboards and external alerting tools.

For a complete Databricks SQL operational dashboard, use the package in [`docs/dashboards`](dashboards/README.md). It includes page blueprints, filters, widgets and queries for executive overview, reliability, performance, quality, streaming, connectors and governance.

## Historical Tables Cleaned

The retention command operates on:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_locks`
- `ctrl_ingestion_explain`
- `ctrl_ingestion_lineage`
- `ctrl_ingestion_schema_changes`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_annotations`
- `ctrl_ingestion_operations`
- `ctrl_ingestion_access`

It does not clean:

- `ctrl_ingestion_state`
- `ctrl_ingestion_metadata`

## Practices

- Schedule cleanup outside the main ingestion window.
- Run without `--apply` first and review the generated SQL.
- Use `VACUUM` only when the environment Delta retention policy allows it.
- Restrict execution permissions for this command to the platform team.
