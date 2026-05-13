from __future__ import annotations

from pathlib import Path

import lakehouse_ingestion
from lakehouse_ingestion.config import FRAMEWORK_VERSION

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_package_versions_are_synchronized():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["version"] == lakehouse_ingestion.__version__
    assert project["version"] == FRAMEWORK_VERSION


def test_release_metadata_is_present():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["license"] == "MIT"
    assert (ROOT / "LICENSE").exists()
    assert (ROOT / "CHANGELOG.md").exists()
    assert {"Homepage", "Changelog", "Issues"} <= set(project["urls"])


def test_spark_dependencies_are_optional_for_databricks_wheels():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    dependencies = project["dependencies"]
    optional_dependencies = project["optional-dependencies"]

    assert dependencies == ["PyYAML>=6.0"]
    assert "pyspark>=3.4,<4" in optional_dependencies["spark"]
    assert "delta-spark>=3.0,<4" in optional_dependencies["spark"]
