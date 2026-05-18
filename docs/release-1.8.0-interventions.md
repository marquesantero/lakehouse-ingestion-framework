# Required Intervention for Library 1.8.0

This document records the only adjustment identified while validating `ingestpacktest` with `contractforge 1.8.0`.

Status in the main repository: applied in `1.8.1`.

## 1. Wheel Packaging for Databricks Serverless

### Symptom

The first regression run failed before executing notebooks:

- job: `contractforge_regression`
- failed run: `850717785525210`
- task: `validate_package_metadata`
- error: `Library installation failed`

Databricks Serverless tried to install the wheel and resolve external dependencies declared by the package.

### Cause

The real library `pyproject.toml` declared Spark and Delta as mandatory dependencies:

```toml
dependencies = [
    "pyspark>=3.4,<4",
    "delta-spark>=3.0,<4",
    "PyYAML>=6.0",
]
```

In Databricks Serverless, Spark and Delta are already part of the runtime. When a wheel declares `pyspark` and `delta-spark` as mandatory dependencies, the installer may try to fetch or resolve those packages before the notebook starts.

### Adjustment Applied in the Test Project

The vendored copy used by the harness kept only `PyYAML` as a mandatory dependency and moved Spark/Delta to an optional extra:

```toml
dependencies = [
    "PyYAML>=6.0",
]

[project.optional-dependencies]
databricks = ["databricks-sdk>=0.20"]
spark = ["pyspark>=3.4,<4", "delta-spark>=3.0,<4"]
```

After that:

- `databricks bundle deploy --target dev --auto-approve`: `Deployment complete`
- `contractforge_regression`: `SUCCESS`
- `contractforge_medallion`: `SUCCESS`

### Recommendation for the Main Repository

Apply the same packaging model in the main library `pyproject.toml`.

Reason: this does not change runtime behavior in Databricks; it only prevents the wheel from trying to install dependencies that the runtime already provides.

### Expected Impact

- Serverless: wheel installation no longer fails because of `pyspark`/`delta-spark` resolution.
- Local development: users who need local Spark can install the `spark` extra.
- PyPI/GitHub releases: package metadata becomes more appropriate for Databricks usage.

## 2. Validation Status After the Adjustment

Local validation:

```text
pytest -q
33 passed

databricks bundle validate --target dev
Validation OK
```

Databricks validation:

```text
contractforge_regression
run_id: 574848419724760
result: SUCCESS
failed_assertions: 0

contractforge_medallion
run_id: 565114273939624
result: SUCCESS
failed_assertions: 0
```
