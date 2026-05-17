from __future__ import annotations

import json

import contractforge.cost as cost_module
from contractforge.cli import main
from contractforge.cost import CostModel, analyze_operational_cost, build_operational_cost_query


def test_build_operational_cost_query_groups_and_estimates_cost():
    query = build_operational_cost_query(
        "main",
        "ops",
        lookback_days=14,
        group_by=["contract_domain", "criticality"],
        cost_model=CostModel(dbu_per_hour=2.5, currency_per_dbu=0.55, currency="USD"),
        include_failed=False,
    )

    assert "FROM `main`.`ops`.`ctrl_ingestion_runs`" in query
    assert "run_date >= date_sub(current_date(), 14)" in query
    assert "AND status = 'SUCCESS'" in query
    assert "`contract_domain`" in query
    assert "`criticality`" in query
    assert "1.375" in query
    assert "'USD' AS estimated_currency" in query
    assert "estimated_cost_per_million_rows" in query


def test_build_operational_cost_query_without_cost_model_keeps_cost_null():
    query = build_operational_cost_query("main", "ops")

    assert "NULL AS estimated_hourly_rate" in query
    assert "'USD' AS estimated_currency" in query
    assert "cost_source" in query


def test_build_operational_cost_query_rejects_invalid_group_by():
    try:
        build_operational_cost_query("main", "ops", group_by=["target_table", "secret"])
    except ValueError as exc:
        assert "group_by desconhecido" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")


def test_analyze_operational_cost_query_only_does_not_execute_spark(monkeypatch):
    class FakeSpark:
        def sql(self, command):  # pragma: no cover
            raise AssertionError(command)

    monkeypatch.setattr(cost_module, "spark", FakeSpark())

    result = analyze_operational_cost(
        "main",
        "ops",
        lookback_days=7,
        group_by=["target_table"],
        query_only=True,
    )

    assert result["status"] == "QUERY_ONLY"
    assert result["rows"] == []
    assert result["group_by"] == ["target_table"]


def test_analyze_operational_cost_executes_and_collects(monkeypatch):
    executed = []

    class FakeRow:
        def asDict(self, recursive=False):
            return {"target_table": "main.silver.orders", "estimated_compute_cost": 1.2}

    class FakeFrame:
        def limit(self, value):
            executed.append(("limit", value))
            return self

        def collect(self):
            return [FakeRow()]

    class FakeSpark:
        def sql(self, command):
            executed.append(("sql", command))
            return FakeFrame()

    monkeypatch.setattr(cost_module, "spark", FakeSpark())

    result = analyze_operational_cost("main", "ops", limit=10)

    assert result["status"] == "SUCCESS"
    assert result["rows"] == [{"target_table": "main.silver.orders", "estimated_compute_cost": 1.2}]
    assert executed[1] == ("limit", 10)


def test_cli_maintenance_cost_report_query_only(capsys):
    assert (
        main(
            [
                "maintenance",
                "cost-report",
                "--catalog",
                "main",
                "--ctrl-schema",
                "ops",
                "--lookback-days",
                "3",
                "--group-by",
                "target_table",
                "--dbu-per-hour",
                "1.5",
                "--currency-per-dbu",
                "0.4",
                "--query-only",
                "--indent",
                "0",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "QUERY_ONLY"
    assert payload["cost_model"]["hourly_rate"] == 0.6000000000000001
    assert payload["group_by"] == ["target_table"]
