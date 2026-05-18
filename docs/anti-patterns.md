# Anti-Patterns

This page lists configurations that look valid but usually cause data loss, excessive cost or weak governance.

## Incremental Snapshot

Wrong:

```yaml
mode: snapshot_soft_delete
watermark_columns: updated_at
merge_keys: device_id
```

`snapshot_soft_delete` requires a complete source. If the source is filtered by watermark, ContractForge cannot distinguish a deleted record from a record that was not read.

Use:

```yaml
mode: snapshot_soft_delete
merge_keys: device_id
source:
  type: connector
  connector: table
  table: main.raw.devices_snapshot
  read:
    source_complete: true
```

## Literal Secrets in YAML

Wrong:

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: jdbc:postgresql://host/db
    user: app_user
    password: plain-text-password
```

Use secret placeholders:

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
```

ContractForge redacts metadata before persisting it to control tables, but contracts should not contain literal secrets when a secret manager is available.

## Exploding Too Early

Avoid changing cardinality in Bronze unless that is an explicit design choice:

```yaml
layer: bronze
shape:
  arrays:
    items:
      mode: explode
```

Prefer preserving the raw payload in Bronze and normalizing it in Silver:

```yaml
layer: silver
shape:
  allow_cardinality_change_on_bronze: false
  arrays:
    items:
      mode: explode_outer
```

## Nullable Merge Key

Wrong:

```yaml
mode: scd1_upsert
merge_keys: customer_id
quality_rules:
  not_null: []
```

Use an explicit quality gate:

```yaml
mode: scd1_upsert
merge_keys: customer_id
quality_rules:
  not_null: [customer_id]
  unique_key: [customer_id]
```

Null keys in `MERGE` make results hard to audit and can hide source-system defects.

## Permanent Explain Mode in Production

Wrong:

```yaml
explain_mode: true
explain_format: formatted
```

Use `explain_mode` in development, CI or targeted diagnostics. In continuous runs, Spark plans can become large and expensive to capture.

## REST API as a Bulk Loader

Wrong:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://api.example.com/all-events
  limits:
    max_pages: 100000
```

For high-volume ingestion, land files in storage first and ingest them with Auto Loader:

```yaml
source:
  type: connector
  connector: autoloader
  format: json
  path: /Volumes/main/landing/events
  read:
    schema_location: /Volumes/main/ops/schemas/events
    checkpoint_location: /Volumes/main/ops/checkpoints/events
```

## Aggressive Access Reconciliation

Avoid starting with automatic revocation:

```yaml
access_policy:
  mode: apply
  on_drift: reconcile
  revoke_unmanaged: true
```

Validate first:

```yaml
access_policy:
  mode: validate_only
  on_drift: warn
  revoke_unmanaged: false
```

When revocation is required, run `contractforge apply-access --force-revoke` during a controlled change window.
