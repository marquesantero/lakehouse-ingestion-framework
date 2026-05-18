# Project Template

The `examples/project_template/` directory contains a minimal structure for starting a ContractForge project with Databricks Asset Bundles.

For richer examples that can be validated by CLI, also see `examples/playground/`.

## Structure

```text
examples/project_template/
  README.md
  databricks.yml
  contracts/
    bronze/
      b_orders.ingestion.yaml
    silver/
      c_orders.ingestion.yaml
      c_orders.annotations.yaml
      c_orders.operations.yaml
      c_orders.access.yaml
  notebooks/
    run_contract.py
```

## Expected Usage

1. Copy the template into a new data repository.
2. Adjust `bundle.name`, `workspace.root_path`, `catalog`, schemas and paths.
3. Publish the ContractForge wheel to a Volume or package registry.
4. Create new contracts with `contractforge init --output contracts/silver/c_orders --source bronze.b_orders --target-table c_orders --layer silver --mode scd1_upsert --merge-keys order_id --split`.
5. Run `contractforge validate-project contracts` locally or in CI.
6. Run the generic notebook with the contract path as a parameter.

## Playground

The `examples/playground/` directory contains ready-to-copy contracts for common scenarios:

- Incremental REST API to Bronze.
- Auto Loader JSON `available_now`.
- Incremental JDBC with SCD1.
- Snapshot with soft delete.
- SCD2 history.
- Gold KPI full refresh.

Validate all examples:

```bash
python examples/playground/scripts/validate_playground.py
```

Or directly:

```bash
contractforge validate-project examples/playground/contracts
```

## Principle

The notebook should stay generic. Ingestion logic belongs in YAML:

```python
from contractforge import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle(dbutils.widgets.get("contract"))
result = ingest_bundle(bundle)
display(result)
```

`ingest_bundle()` raises `ContractForgeExecutionError` by default when execution finishes with `FAILED` or `ABORTED`. The failure is still recorded in control tables before the exception is raised. Use `ingest_bundle(bundle, raise_on_failure=False)` only when a notebook or test must inspect the failed payload directly.

This avoids one notebook per table and keeps pipeline changes reviewable in pull requests.
