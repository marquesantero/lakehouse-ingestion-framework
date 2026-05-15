from __future__ import annotations

from pathlib import Path

from contractforge.cli import main


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


def run() -> int:
    print(f"Validando contratos em {CONTRACTS}")
    return main(["validate-project", str(CONTRACTS), "--indent", "2"])


if __name__ == "__main__":
    raise SystemExit(run())
