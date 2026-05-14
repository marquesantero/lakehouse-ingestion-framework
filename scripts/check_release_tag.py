"""Validate that a Git tag matches the package version.

Usage:
    python scripts/check_release_tag.py v1.13.0
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: check_release_tag.py <tag>", file=sys.stderr)
        return 2
    tag = args[0].strip()
    version = str(tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"])
    expected = f"v{version}"
    if tag != expected:
        print(f"release-tag-check: tag {tag!r} does not match expected {expected!r}", file=sys.stderr)
        return 1
    print(f"release-tag-check: ok ({tag})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
