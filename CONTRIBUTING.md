# Contributing to ContractForge

Thank you for considering a contribution. ContractForge is a contract-first ingestion framework, so changes should preserve operational safety, explicit contracts, and clear documentation.

## Ways to Contribute

- Report bugs with a minimal contract, runtime details, and the observed control-table output when available.
- Propose features through an issue before opening a large implementation PR.
- Improve documentation, examples, templates, tests, and compatibility notes.
- Add connectors or ingestion features only when the behavior is general enough for multiple users and runtimes.

## Development Setup

```bash
git clone https://github.com/marquesantero/contractforge.git
cd contractforge
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

On Linux/macOS, use `source .venv/bin/activate`.

## Validation Before a PR

Run the fast validation suite before opening a PR:

```bash
ruff check .
pytest -q
python scripts/check_release.py
```

If Spark or Java is unavailable, run the pure test suite:

```bash
SKIP_SPARK_TESTS=1 pytest -q
```

On Windows PowerShell:

```powershell
$env:SKIP_SPARK_TESTS = "1"
pytest -q
```

## Pull Request Expectations

- Keep PRs focused on one theme: bug fix, connector, documentation, refactor, or release tooling.
- Include tests for behavior changes.
- Update documentation and YAML/Python examples when adding or changing user-facing behavior.
- Do not include credentials, SAS tokens, workspace URLs with secrets, customer data, or generated cache files.
- Prefer defensive validation over runtime surprises.
- Avoid Databricks-specific assumptions unless the feature is explicitly Databricks-only and documented as such.

## GitHub Project Workflow

ContractForge uses the GitHub Project `ContractForge` as the operational source of truth for planned work, active work, and historical delivery records.

- Every relevant action must have a Project item. Prefer repository issues instead of draft cards so work keeps labels, milestones, discussion, PR links, and audit history.
- Create the item before implementation when the work is more than a trivial typo fix. Include context, scope, acceptance criteria, expected validation, risk notes, and links to related docs, runs, issues, or PRs.
- Assign labels that describe both type and area, for example `type:backlog`, `type:technical-debt`, `area:core`, `area:connectors`, `area:observability`, `area:shape`, `area:security`, or `area:docs`.
- Assign the appropriate milestone/sprint. Closed milestones represent completed historical work; open milestones represent active or planned work.
- Set `Priority` and `Size` in the Project when creating or triaging the item.

Project status rules:

- `Backlog`: accepted idea or future work, but not selected for immediate execution.
- `Ready`: next action has been selected and the implementation/validation approach is clear.
- `In progress`: implementation or active investigation has started.
- `In review`: PR is open, review is pending, or validation results are being checked before merge.
- `Done`: implementation, documentation, validation, merge, and release steps are complete when applicable.

When finishing an item, update the issue or PR with the evidence that matters: commands run, Databricks job/run IDs, test results, release/tag links, documentation links, known residual risks, and any follow-up issues created from the work.

## Compatibility Principles

- `main` is protected and changes must go through PR.
- Required checks are `build`, `test (3.10)`, and `test (3.11)`.
- The package targets Python 3.10+.
- Spark/Delta dependencies are optional for local use and are not mandatory wheel dependencies because Databricks provides them at runtime.

## Release Process

Maintainers publish releases from versioned tags:

```bash
python -m build
twine check dist/*
git tag vX.Y.Z
git push origin vX.Y.Z
```

The release workflow checks package metadata, verifies the tag matches the package version, builds the artifacts, and attaches them to the GitHub Release.

## Design Notes

For larger changes, document the rationale in `docs/adrs/` when the decision changes architecture, runtime compatibility, governance behavior, or operational guarantees.
