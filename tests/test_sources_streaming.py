from __future__ import annotations

import pytest

import contractforge.ingestion as ingestion_module
import contractforge.streaming as streaming_module
from contractforge.ingestion import ingest_plan
from contractforge.plan import SourceSpec, build_plan_from_kwargs
from contractforge.sources import (
    AutoloaderResolver,
    get_source_resolver,
    register_source_resolver,
)


def test_register_and_get_source_resolver():
    class Resolver:
        def resolve_stream(self, spec, plan):
            return None, "test"

    resolver = Resolver()
    register_source_resolver("unit_test_source", resolver)
    assert get_source_resolver("unit_test_source") is resolver


def test_get_source_resolver_rejects_unknown():
    with pytest.raises(ValueError, match="não tem resolver"):
        get_source_resolver("missing_source")


def test_autoloader_resolver_uses_read_stream_and_options(monkeypatch):
    calls = {"format": None, "options": {}, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def option(self, key, value):
            calls["options"][key] = value
            return self

        def options(self, **kwargs):
            calls["options"].update(kwargs)
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        readStream = Reader()

    monkeypatch.setattr("contractforge.sources.spark", FakeSpark())
    spec = SourceSpec(
        type="autoloader",
        path="/landing/orders",
        format="json",
        schema_location="/schemas/orders",
        checkpoint_location="/checkpoints/orders",
        schema_hints="id BIGINT",
        options={"cloudFiles.inferColumnTypes": "true"},
        max_files_per_trigger=5,
    )

    df, label = AutoloaderResolver().resolve_stream(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert df == "df"
    assert label == "autoloader:/landing/orders"
    assert calls["format"] == "cloudFiles"
    assert calls["load"] == "/landing/orders"
    assert calls["options"]["cloudFiles.format"] == "json"
    assert calls["options"]["cloudFiles.schemaLocation"] == "/schemas/orders"
    assert calls["options"]["cloudFiles.includeExistingFiles"] == "true"
    assert calls["options"]["cloudFiles.schemaHints"] == "id BIGINT"
    assert calls["options"]["cloudFiles.maxFilesPerTrigger"] == "5"
    assert calls["options"]["cloudFiles.inferColumnTypes"] == "true"


def test_ingest_plan_dispatches_source_spec_to_stream(monkeypatch):
    plan = build_plan_from_kwargs(
        source={
            "type": "autoloader",
            "path": "/landing/orders",
            "schema_location": "/schemas/orders",
            "checkpoint_location": "/checkpoints/orders",
        },
        target_table="b_orders",
        lock_enabled=False,
    )

    def fake_stream(inner_plan, *, raise_on_failure=True):
        return {
            "status": "DRY_RUN",
            "stream_run_id": "stream-1",
            "source": inner_plan.source.path,
            "raise_on_failure": raise_on_failure,
        }

    monkeypatch.setattr("contractforge.ingestion.ingest_stream_plan", fake_stream)

    assert ingest_plan(plan) == {
        "status": "DRY_RUN",
        "stream_run_id": "stream-1",
        "source": "/landing/orders",
        "raise_on_failure": True,
    }


def test_stream_metrics_from_batches_normalizes_result_keys():
    metrics = streaming_module._stream_metrics_from_batches(
        [
            {"rows_read": 2, "rows_written": 2, "rows_quarantined": 1},
            {
                "total_rows_read": 1,
                "total_rows_written": 1,
                "total_rows_quarantined": 0,
            },
        ]
    )

    assert metrics == {
        "batches_processed": 2,
        "total_rows_read": 3,
        "total_rows_written": 3,
        "total_rows_quarantined": 1,
    }


def test_stream_metrics_prefers_child_when_local_result_is_incomplete():
    local = {
        "batches_processed": 1,
        "total_rows_read": 0,
        "total_rows_written": 0,
        "total_rows_quarantined": 0,
    }
    child = {
        "batches_processed": 1,
        "total_rows_read": 3,
        "total_rows_written": 3,
        "total_rows_quarantined": 0,
    }

    assert streaming_module._prefer_child_stream_metrics(local, child) is True


def test_ingest_stream_plan_uses_child_run_metrics_fallback(monkeypatch):
    finish_payloads = []

    class Query:
        def awaitTermination(self):
            return None

    class Writer:
        def foreachBatch(self, callback):
            self.callback = callback
            return self

        def option(self, key, value):
            return self

        def trigger(self, availableNow):
            return self

        def start(self):
            return Query()

    class StreamDataFrame:
        @property
        def writeStream(self):
            return Writer()

    class Resolver:
        def resolve_stream(self, spec, plan):
            return StreamDataFrame(), f"{spec.type}:{spec.path}"

    plan = build_plan_from_kwargs(
        source={
            "type": "autoloader",
            "path": "/landing/orders",
            "schema_location": "/schemas/orders",
            "checkpoint_location": "/checkpoints/orders",
        },
        target_table="b_orders",
    )

    child_metrics = {
        "batches_processed": 1,
        "total_rows_read": 3,
        "total_rows_written": 3,
        "total_rows_quarantined": 0,
    }
    monkeypatch.setattr(
        streaming_module,
        "runtime_info",
        lambda: {"runtime_type": "unit", "spark_version": "test", "python_version": "test"},
    )
    monkeypatch.setattr(
        streaming_module,
        "ensure_ctrl_tables",
        lambda catalog, ctrl_schema: {"runs": "runs", "streams": "streams"},
    )
    monkeypatch.setattr(streaming_module, "find_idempotent_stream", lambda *args, **kwargs: None)
    monkeypatch.setattr(streaming_module, "log_stream_start", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        streaming_module,
        "log_stream_finish",
        lambda tables, stream_run_id, payload: finish_payloads.append(payload),
    )
    monkeypatch.setattr(streaming_module, "stream_child_run_metrics", lambda *args: child_metrics)
    monkeypatch.setattr(streaming_module, "get_source_resolver", lambda source_type: Resolver())

    result = ingestion_module.ingest_stream_plan(plan)

    assert result["status"] == "SUCCESS"
    assert result["batches_processed"] == 1
    assert result["total_rows_read"] == 3
    assert result["total_rows_written"] == 3
    assert result["total_rows_quarantined"] == 0
    assert finish_payloads[0]["batches_processed"] == 1
    assert finish_payloads[0]["total_rows_written"] == 3
