from __future__ import annotations

from pathlib import Path

from contractforge.cli import main


ROOT = Path(__file__).resolve().parents[1]
PLAYGROUND = ROOT / "examples" / "playground"


def test_playground_contracts_validate_as_project(capsys):
    assert main(["validate-project", str(PLAYGROUND / "contracts"), "--indent", "0"]) == 0
    output = capsys.readouterr().out
    assert '"status": "SUCCESS"' in output
    assert "b_orders_api.ingestion.yaml" in output
    assert "g_daily_orders.ingestion.yaml" in output


def test_playground_validation_script_runs(capsys):
    from examples.playground.scripts.validate_playground import run

    assert run() == 0
    assert "Validando contratos" in capsys.readouterr().out
