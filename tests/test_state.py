from __future__ import annotations

from contractforge import state


def test_record_ctrl_metadata_uses_retry(monkeypatch):
    calls = {"retry": 0, "sql": 0}

    def fake_retry(fn, attempts=3, backoff_seconds=5):
        calls["retry"] += 1
        return fn()

    class FakeSpark:
        def sql(self, query):
            calls["sql"] += 1
            assert "MERGE INTO" in query

    monkeypatch.setattr(state, "with_retry", fake_retry)
    monkeypatch.setattr(state, "spark", FakeSpark())

    state._record_ctrl_metadata({"metadata": "main.ops.ctrl_ingestion_metadata"})

    assert calls == {"retry": 1, "sql": 1}
