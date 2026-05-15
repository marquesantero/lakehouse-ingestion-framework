# Support

ContractForge is maintained as an open-source project. Support is issue-driven and depends on the quality of the reproduction.

## Before Opening an Issue

- Check the documentation site: https://marquesantero.github.io/contractforge/
- Check `docs/compatibilidade_conectores.md` for connector/runtime requirements.
- Check `docs/antipadroes.md` for common configuration mistakes.
- Run `contractforge validate` or `contractforge validate-bundle` when the issue involves YAML contracts.

## Good Support Requests Include

- ContractForge version.
- Databricks Runtime, classic/serverless, or local Spark details.
- Connector or write mode used.
- Minimal YAML/Python example.
- Error message and stack trace with secrets removed.
- Relevant `ctrl_ingestion_runs`, `ctrl_ingestion_errors`, or `ctrl_ingestion_streams` fields when available.

## Security Issues

Do not use public issues for security reports. Follow `SECURITY.md`.
