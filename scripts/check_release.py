"""Release consistency checks for the package.

This script intentionally uses only the Python standard library so it can run
in CI before optional development dependencies are installed.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def _literal_assign(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = ast.literal_eval(node.value)
                    return str(value)
    raise AssertionError(f"{name} not found in {path}")


def main() -> int:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject["project"]
    package_version = str(project["version"])
    init_version = _literal_assign(ROOT / "src/contractforge/__init__.py", "__version__")
    framework_version = _literal_assign(ROOT / "src/contractforge/config.py", "FRAMEWORK_VERSION")

    errors = []
    if package_version != init_version:
        errors.append(f"pyproject version {package_version} != __version__ {init_version}")
    if package_version != framework_version:
        errors.append(f"pyproject version {package_version} != FRAMEWORK_VERSION {framework_version}")
    if not (ROOT / "LICENSE").exists():
        errors.append("LICENSE file is missing")
    if f"## {package_version} " not in (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"):
        errors.append(f"CHANGELOG.md has no section for {package_version}")
    if project.get("license") != "MIT":
        errors.append("pyproject license must use SPDX string 'MIT'")
    for url_name in ("Homepage", "Changelog", "Issues"):
        if url_name not in project.get("urls", {}):
            errors.append(f"project.urls.{url_name} is missing")

    if errors:
        for error in errors:
            print(f"release-check: {error}", file=sys.stderr)
        return 1
    print(f"release-check: ok ({package_version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
