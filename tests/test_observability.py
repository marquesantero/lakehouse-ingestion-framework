from lakehouse_ingestion.config import CTRL_SCHEMA_VERSION, FRAMEWORK_VERSION
from lakehouse_ingestion.ingestion import _short_error_message
from lakehouse_ingestion.plan import build_plan_from_kwargs
from lakehouse_ingestion.writers import logical_row_metrics, resolve_write_metrics


def test_short_error_message_uses_last_traceback_line():
    traceback_text = (
        "Traceback (most recent call last):\n"
        "  File \"job.py\", line 1, in <module>\n"
        "ValueError: invalid contract\n"
    )

    assert _short_error_message(traceback_text) == "ValueError: invalid contract"


def test_framework_and_ctrl_schema_versions_are_current():
    assert FRAMEWORK_VERSION == "1.8.0"
    assert CTRL_SCHEMA_VERSION == 9


def test_logical_row_metrics_for_append_like_mode():
    plan = build_plan_from_kwargs(source="x", target_table="t", mode="scd1_hash_diff")

    assert logical_row_metrics(plan, 7) == {
        "rows_inserted": 7,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_affected": 7,
    }


def test_resolve_write_metrics_preserves_delta_metrics_and_adds_logical_metrics():
    plan = build_plan_from_kwargs(source="x", target_table="t", mode="scd1_upsert", merge_keys="id")
    delta_metrics = {
        "version": 12,
        "operation": "MERGE",
        "operationMetrics": {"numTargetRowsInserted": "2", "numTargetRowsUpdated": "3"},
    }

    row_metrics, operation_metrics, metrics_source = resolve_write_metrics(plan, 5, delta_metrics)

    assert metrics_source == "mixed"
    assert row_metrics["rows_inserted"] == 2
    assert row_metrics["rows_updated"] == 3
    assert row_metrics["rows_affected"] == 5
    assert operation_metrics["logicalMetrics"]["rows_affected"] == 5
