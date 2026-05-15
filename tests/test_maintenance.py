from __future__ import annotations

import json

import contractforge.maintenance as maintenance_module
from contractforge.cli import main
from contractforge.maintenance import apply_ctrl_retention, build_ctrl_retention_plan


def test_build_ctrl_retention_plan_is_dry_and_quotes_tables():
    plan = build_ctrl_retention_plan(
        "main",
        "ops",
        retention_days=90,
        vacuum=True,
        vacuum_retention_hours=240,
        targets=["runs", "lineage"],
    )

    assert [item["target"] for item in plan] == ["runs", "lineage"]
    assert plan[0]["table"] == "main.ops.ctrl_ingestion_runs"
    assert "DELETE FROM `main`.`ops`.`ctrl_ingestion_runs`" in plan[0]["commands"][0]
    assert "`run_date` < date_sub(current_date(), 90)" in plan[0]["commands"][0]
    assert plan[0]["commands"][1] == "VACUUM `main`.`ops`.`ctrl_ingestion_runs` RETAIN 240 HOURS"
    assert "event_time_utc < current_timestamp() - INTERVAL 90 DAYS" in plan[1]["commands"][0]


def test_apply_ctrl_retention_executes_when_apply(monkeypatch):
    executed = []

    class FakeSpark:
        def sql(self, command):
            executed.append(command)

    monkeypatch.setattr(maintenance_module, "spark", FakeSpark())

    result = apply_ctrl_retention(
        "main",
        "ops",
        retention_days=30,
        dry_run=False,
        targets=["quarantine"],
    )

    assert result["status"] == "SUCCESS"
    assert result["targets"] == ["quarantine"]
    assert executed == result["executed_commands"]
    assert len(executed) == 1
    assert "ctrl_ingestion_quarantine" in executed[0]


def test_cli_maintenance_ctrl_retention_defaults_to_dry_run(capsys):
    assert (
        main(
            [
                "maintenance",
                "ctrl-retention",
                "--catalog",
                "main",
                "--ctrl-schema",
                "ops",
                "--retention-days",
                "45",
                "--target",
                "runs",
                "--indent",
                "0",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "DRY_RUN"
    assert payload["targets"] == ["runs"]
    assert payload["executed_commands"] == []


def test_cli_maintenance_ctrl_retention_rejects_invalid_target(capsys):
    assert (
        main(
            [
                "maintenance",
                "ctrl-retention",
                "--retention-days",
                "45",
                "--target",
                "state",
            ]
        )
        == 1
    )
    assert "desconhecidos" in capsys.readouterr().err
