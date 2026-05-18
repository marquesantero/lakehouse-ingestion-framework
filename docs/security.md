# Security and Secrets

This guide summarizes where sensitive data may appear and which practices should be used when operating ContractForge.

## Secrets

Use placeholders:

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
```

Resolution order:

- First, ContractForge tries the environment variable `CONTRACTFORGE_SECRET_<SCOPE>_<KEY>`.
- If it does not exist, it tries Databricks Secrets through `dbutils.secrets.get(scope, key)`.
- Structures written to logs and control tables are redacted before persistence.

## Redacted Fields

Keys are treated as sensitive when they contain terms such as:

- `authorization`
- `password`
- `secret`
- `token`
- `api_key`
- `apikey`
- `key`

Values in the `{{ secret:scope/key }}` format are also redacted.

Tracebacks and error messages are redacted before they are written to logs, `ingest()` results, `ctrl_ingestion_runs`, `ctrl_ingestion_errors` and `ctrl_ingestion_state`. The full stack trace remains available for diagnostics, but sensitive patterns are replaced by `***REDACTED***`.

## Free-text Redaction

In addition to structured dictionaries, ContractForge redacts sensitive patterns in free text before writing audit data. This mainly protects `ctrl_ingestion_explain` and `ctrl_ingestion_lineage`, where Spark connectors may include options in physical plans or operational metrics.

Covered patterns include:

- `{{ secret:scope/key }}` placeholders.
- `Bearer <token>` and `Basic <token>` headers.
- URLs with user/password, such as `jdbc:postgresql://user:password@host/db`.
- Query strings or JDBC parameters such as `?password=...`, `;token=...`, `&api_key=...`.
- Text assignments such as `password=...`, `token=...`, `client_secret=...`, `authorization=...`.

## Explain and Lineage

- Use `explain_mode` for diagnostics, not as permanent production logging.
- Avoid sensitive literals in `source.query`, `filter_expression`, `dedup_order_expr` or quality expressions.
- OpenLineage events should carry operational metadata, not credentials or business payloads. Events are redacted before persistence, but OpenLineage should not be used as a sensitive payload channel.
- If an external connector requires sensitive options with non-standard names, prefer names containing `secret`, `token`, `password` or `key` so automatic redaction applies.
- Connector metadata, including `source_path`, `source_table`, labels and serialized options, is redacted before being written to control tables.

## Redaction Audit

The test suite covers:

- Recursive redaction of dictionaries, lists and tuples.
- `{{ secret:scope/key }}` placeholders.
- `Bearer` and `Basic` headers.
- URLs with user/password.
- Query strings and JDBC parameters with `password`, `token`, `api_key` and similar names.
- REST/JDBC connector metadata before persistence in `ctrl_ingestion_runs`.

When creating a custom connector, do not write credentials directly to `metadata`. Return operational metadata and use standard sensitive names for any field that must be redacted.

## Control Tables

Restrict access to the `ops` schema:

- `ctrl_ingestion_runs` contains source names, targets, redacted parameters and error messages.
- `ctrl_ingestion_errors` may contain full stack traces.
- `ctrl_ingestion_quarantine` may contain rejected payloads and should follow the same access policy as the source data.
- `ctrl_ingestion_lineage` can reveal data topology.

## Checklist

- Use Databricks Secrets or environment variables; never store literal secrets in YAML.
- Review `source.query` and expressions to avoid sensitive literals.
- Apply restricted grants to the `ops` schema.
- Treat quarantine data as sensitive.
- Use `annotations.columns.<column>.pii` to mark PII and support audits.
