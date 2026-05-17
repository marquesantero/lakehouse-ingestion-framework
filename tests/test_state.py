from __future__ import annotations

import pytest

from contractforge import state


class _FakeSqlResult:
    def __init__(self, exists: bool):
        self.exists = exists

    def where(self, *_args, **_kwargs):
        return self

    def select(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def first(self):
        return object() if self.exists else None


class _FakeSpark:
    def __init__(self, metadata_exists_sequence):
        self.metadata_exists_sequence = list(metadata_exists_sequence)
        self.sql_calls = []

    def sql(self, query):
        self.sql_calls.append(query)
        if "SELECT 1" in query:
            exists = self.metadata_exists_sequence.pop(0)
            return _FakeSqlResult(exists)
        return None


def test_record_ctrl_metadata_skips_when_current(monkeypatch):
    fake_spark = _FakeSpark([True])

    monkeypatch.setattr(state, "spark", fake_spark)

    state._record_ctrl_metadata({"metadata": "main.ops.ctrl_ingestion_metadata"})

    assert len(fake_spark.sql_calls) == 1
    assert "SELECT 1" in fake_spark.sql_calls[0]


def test_record_ctrl_metadata_uses_retry_when_missing(monkeypatch):
    calls = {"retry": 0}
    fake_spark = _FakeSpark([False])

    def fake_retry(fn, attempts=3, backoff_seconds=5):
        calls["retry"] += 1
        return fn()

    monkeypatch.setattr(state, "with_retry", fake_retry)
    monkeypatch.setattr(state, "spark", fake_spark)

    state._record_ctrl_metadata({"metadata": "main.ops.ctrl_ingestion_metadata"})

    assert calls == {"retry": 1}
    assert len(fake_spark.sql_calls) == 2
    assert "SELECT 1" in fake_spark.sql_calls[0]
    assert "MERGE INTO" in fake_spark.sql_calls[1]


def test_record_ctrl_metadata_ignores_concurrent_conflict_when_current_after_retry(monkeypatch):
    fake_spark = _FakeSpark([False, True])

    def fake_retry(_fn, attempts=3, backoff_seconds=5):
        raise RuntimeError("ConcurrentAppendException: DELTA_CONCURRENT_APPEND")

    monkeypatch.setattr(state, "with_retry", fake_retry)
    monkeypatch.setattr(state, "spark", fake_spark)

    state._record_ctrl_metadata({"metadata": "main.ops.ctrl_ingestion_metadata"})

    select_calls = [query for query in fake_spark.sql_calls if "SELECT 1" in query]
    assert len(select_calls) == 2


def test_record_ctrl_metadata_reraises_concurrent_conflict_when_still_missing(monkeypatch):
    fake_spark = _FakeSpark([False, False])

    def fake_retry(_fn, attempts=3, backoff_seconds=5):
        raise RuntimeError("ConcurrentAppendException: DELTA_CONCURRENT_APPEND")

    monkeypatch.setattr(state, "with_retry", fake_retry)
    monkeypatch.setattr(state, "spark", fake_spark)

    with pytest.raises(RuntimeError, match="ConcurrentAppendException"):
        state._record_ctrl_metadata({"metadata": "main.ops.ctrl_ingestion_metadata"})
