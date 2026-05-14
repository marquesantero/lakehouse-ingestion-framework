from __future__ import annotations

import pytest

import lakehouse_ingestion.sources as sources_module
from lakehouse_ingestion.ingestion import _validate_static_plan_options, ingest_plan
from lakehouse_ingestion.plan import ConnectorSpec, build_plan_from_kwargs
from lakehouse_ingestion.sources import (
    ConnectorCapabilities,
    JdbcConnector,
    RestApiConnector,
    SourceResolution,
    list_source_resolvers,
    redact_secrets,
    register_source_resolver,
    resolve_batch_source,
)


def test_build_plan_accepts_connector_source():
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "json",
            "name": "orders_json",
            "path": "/landing/orders",
            "options": {"multiline": True},
            "read": {"source_complete": True},
        },
        target_table="b_orders",
    )

    assert isinstance(plan.source, ConnectorSpec)
    assert plan.source.connector == "json"
    assert plan.source.name == "orders_json"
    assert plan.source.read["source_complete"] is True


def test_build_plan_accepts_custom_connector_names():
    plan = build_plan_from_kwargs(
        source={"type": "connector", "connector": "salesforce", "name": "crm_accounts"},
        target_table="b_accounts",
    )

    assert isinstance(plan.source, ConnectorSpec)
    assert plan.source.connector == "salesforce"


def test_build_plan_rejects_invalid_connector_name():
    with pytest.raises(ValueError, match="source.connector"):
        build_plan_from_kwargs(
            source={"type": "connector", "connector": "123 invalid", "path": "/x"},
            target_table="b_orders",
        )


def test_builtin_connectors_are_registered():
    registered = set(list_source_resolvers())

    assert {"json", "parquet", "csv", "object_storage", "blob", "jdbc", "rest_api", "sql", "table"} <= registered


def test_custom_connector_can_be_registered_and_resolved():
    class CustomConnector:
        def capabilities(self, spec):
            return ConnectorCapabilities(batch=True, source_complete=True)

        def resolve_batch(self, spec, plan):
            capabilities = self.capabilities(spec)
            return SourceResolution(
                df={"custom": spec.name},
                label=f"{spec.connector}:{spec.name}",
                connector=spec.connector,
                metadata={"source_connector": spec.connector},
                capabilities=capabilities,
            )

    register_source_resolver("custom_unit_source", CustomConnector())
    plan = build_plan_from_kwargs(
        source={"type": "connector", "connector": "custom_unit_source", "name": "orders"},
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == {"custom": "orders"}
    assert resolved.label == "custom_unit_source:orders"


def test_redact_secrets_recursively():
    payload = {
        "url": "jdbc:test",
        "password": "plain",
        "headers": {"Authorization": "Bearer token"},
        "nested": ["{{ secret:scope/key }}", {"api_key": "abc"}],
    }

    assert redact_secrets(payload) == {
        "url": "jdbc:test",
        "password": "***REDACTED***",
        "headers": {"Authorization": "***REDACTED***"},
        "nested": ["***REDACTED***", {"api_key": "***REDACTED***"}],
    }


def test_file_connector_uses_spark_reader(monkeypatch):
    calls = {"format": None, "options": {}, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            calls["options"].update(kwargs)
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={"type": "connector", "connector": "json", "path": "/landing/orders", "options": {"multiline": True}},
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert resolved.label == "json:/landing/orders"
    assert resolved.connector == "json"
    assert calls == {"format": "json", "options": {"multiline": "true"}, "load": "/landing/orders"}


def test_jdbc_connector_requires_complete_partition_options():
    spec = ConnectorSpec(
        connector="jdbc",
        options={"url": "jdbc:test", "dbtable": "public.orders"},
        read={"partition_column": "id", "lower_bound": 1},
    )

    with pytest.raises(ValueError, match="partitioning requer"):
        JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_jdbc_connector_applies_incremental_predicate(monkeypatch):
    captured = {}

    class Reader:
        def format(self, value):
            captured["format"] = value
            return self

        def options(self, **kwargs):
            captured.update(kwargs)
            return self

        def load(self):
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    spec = ConnectorSpec(
        connector="jdbc",
        options={"url": "jdbc:test", "dbtable": "public.orders"},
        incremental={"watermark_column": "updated_at"},
    )
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        runtime_parameters={"_contractforge_watermark_previous": "2026-05-01T00:00:00Z"},
    )

    resolved = JdbcConnector().resolve_batch(spec, plan)

    assert resolved.df == "df"
    assert captured["format"] == "jdbc"
    assert captured["dbtable"] == (
        "(SELECT * FROM public.orders WHERE updated_at > '2026-05-01T00:00:00Z') cf_src"
    )
    assert resolved.metadata["source_metrics"] == {
        "read_strategy": "jdbc_table",
        "incremental_applied": True,
        "watermark_value": "2026-05-01T00:00:00Z",
        "partitioned_read": False,
        "fetchsize": None,
        "source_complete": False,
    }


def test_snapshot_connector_requires_source_complete():
    plan = build_plan_from_kwargs(
        source={"type": "connector", "connector": "table", "table": "main.raw.orders"},
        target_table="c_orders",
        layer="silver",
        mode="snapshot_soft_delete",
        merge_keys=["id"],
    )

    with pytest.raises(ValueError, match="source.read.source_complete=true"):
        _validate_static_plan_options(plan)


def test_replace_partitions_accepts_connector_source_complete():
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "table",
            "table": "main.raw.orders",
            "read": {"source_complete": True},
        },
        target_table="c_orders",
        layer="silver",
        mode="scd1_upsert",
        merge_keys=["id"],
        merge_strategy="replace_partitions",
        merge_partition_column="ingestion_date",
        partition_column="ingestion_date",
    )

    _validate_static_plan_options(plan)


def test_rest_api_connector_paginates_cursor(monkeypatch):
    responses = [
        ({"data": [{"id": 1}], "next": "abc"}, {}),
        ({"data": [{"id": 2}], "next": None}, {}),
    ]
    requested_urls = []

    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout):
        requested_urls.append(url)
        payload, headers_payload = responses.pop(0)
        return payload, headers_payload, url, len(str(payload).encode("utf-8"))

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)

    spec = ConnectorSpec(
        connector="rest_api",
        name="orders_api",
        request={"url": "https://api.example.com/orders"},
        pagination={"type": "cursor", "cursor_param": "cursor", "next_cursor_path": "$.next"},
        response={"records_path": "$.data"},
        limits={"max_pages": 2},
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df["rows"] == [{"id": 1}, {"id": 2}]
    assert requested_urls == [
        "https://api.example.com/orders",
        "https://api.example.com/orders?cursor=abc",
    ]
    assert resolved.metadata["source_metrics"]["request_count"] == 2
    assert resolved.metadata["source_metrics"]["pages_read"] == 2
    assert resolved.metadata["source_metrics"]["records_read"] == 2
    assert resolved.metadata["source_metrics"]["bytes_read"] > 0
    assert resolved.metadata["source_metrics"]["pagination_type"] == "cursor"


def test_rest_api_connector_applies_params_and_incremental(monkeypatch):
    requested = {}

    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout):
        requested["url"] = url
        requested["headers"] = headers
        requested["body"] = body
        return {"data": [{"id": 1}]}, {}, url, 24

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        name="orders_api",
        request={
            "url": "https://api.example.com/orders",
            "params": {"status": "open"},
            "json": {"source": "contractforge"},
        },
        incremental={
            "watermark_param": "updated_after",
            "watermark_header": "X-Watermark",
            "watermark_body_field": "updated_after",
        },
        response={"records_path": "$.data"},
    )
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        runtime_parameters={"_contractforge_watermark_previous": "2026-05-01T00:00:00Z"},
    )

    resolved = RestApiConnector().resolve_batch(spec, plan)

    assert resolved.df["rows"] == [{"id": 1}]
    assert resolved.metadata["source_incremental_redacted"] == {
        "watermark_param": "updated_after",
        "watermark_header": "X-Watermark",
        "watermark_body_field": "updated_after",
    }
    assert requested["url"] == (
        "https://api.example.com/orders?status=open&updated_after=2026-05-01T00%3A00%3A00Z"
    )
    assert requested["headers"]["X-Watermark"] == "2026-05-01T00:00:00Z"
    assert requested["body"] == b'{"source": "contractforge", "updated_after": "2026-05-01T00:00:00Z"}'
    assert resolved.metadata["source_metrics"]["incremental_applied"] is True
    assert resolved.metadata["source_metrics"]["watermark_value"] == "2026-05-01T00:00:00Z"
    assert resolved.metadata["source_metrics"]["request_count"] == 1
    assert resolved.metadata["source_metrics"]["records_read"] == 1


def test_rest_api_connector_validates_missing_bearer_token():
    spec = ConnectorSpec(
        connector="rest_api",
        request={"url": "https://api.example.com/orders"},
        auth={"type": "bearer_token"},
    )

    with pytest.raises(ValueError, match="auth.token"):
        RestApiConnector()._headers(spec)


def test_ingest_plan_dispatches_autoloader_connector_to_stream(monkeypatch):
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "autoloader",
            "path": "/landing/orders",
            "format": "json",
            "read": {
                "schema_location": "/schemas/orders",
                "checkpoint_location": "/checkpoints/orders",
            },
        },
        target_table="b_orders",
        lock_enabled=False,
    )

    def fake_stream(inner_plan):
        return {
            "status": "DRY_RUN",
            "stream_run_id": "stream-1",
            "source": inner_plan.source.path,
            "checkpoint": inner_plan.source.checkpoint_location,
        }

    monkeypatch.setattr("lakehouse_ingestion.ingestion.ingest_stream_plan", fake_stream)

    assert ingest_plan(plan) == {
        "status": "DRY_RUN",
        "stream_run_id": "stream-1",
        "source": "/landing/orders",
        "checkpoint": "/checkpoints/orders",
    }
