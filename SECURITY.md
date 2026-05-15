# Security Policy

## Supported Versions

Security reports should target the latest released version. Older versions may receive fixes only when the fix is low risk and the affected behavior is still present in the current release line.

## Reporting a Vulnerability

Do not open a public issue for vulnerabilities, leaked credentials, or exploit details.

Use GitHub private vulnerability reporting if available for this repository. If it is not available, contact the repository owner privately through GitHub.

Include:

- A clear description of the vulnerability.
- A minimal reproduction when possible.
- Affected version, runtime, and connector or ingestion mode.
- Whether credentials, secrets, control tables, lineage payloads, or logs are affected.

## Secret Handling

ContractForge redacts common sensitive fields in source metadata, lineage, and control-table payloads. Contributors must preserve this behavior when changing connectors, logging, explain output, OpenLineage events, or error handling.

Never commit:

- SAS tokens, API keys, passwords, OAuth secrets, or JDBC URLs with credentials.
- Databricks workspace tokens.
- Customer data, private datasets, or production control-table exports.

Use `{{ secret:scope/key }}` placeholders in examples.
