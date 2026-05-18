from __future__ import annotations

import pytest

import contractforge.sources as sources_module
from contractforge.ingestion import _validate_static_plan_options, ingest_plan
from contractforge.plan import ConnectorSpec, build_plan_from_kwargs
from contractforge.sources import (
    ConnectorCapabilities,
    FileConnector,
    HttpFileConnector,
    JdbcConnector,
    ObjectStorageConnector,
    RestApiConnector,
    SparkFormatConnector,
    SourceResolution,
    diagnose_source_connectors,
    list_source_resolvers,
    redact_secrets,
    redact_text,
    register_source_resolver,
    resolve_batch_source,
)


def _assert_text_not_present(value, text: str) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _assert_text_not_present(item, text)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_text_not_present(item, text)
        return
    assert text not in str(value)


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

    assert {
        "adls",
        "azure_blob",
        "bigquery",
        "blob",
        "csv",
        "delta",
        "gcs",
        "http_csv",
        "http_file",
        "http_json",
        "http_text",
        "jdbc",
        "json",
        "mysql",
        "object_storage",
        "oracle",
        "orc",
        "parquet",
        "postgres",
        "rest_api",
        "s3",
        "snowflake",
        "sql",
        "sqlserver",
        "table",
    } <= registered


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


def test_redact_text_covers_free_form_secret_patterns():
    text = (
        "url=jdbc:postgresql://user:s3cr3t@host/db?password=topsecret;token=abc "
        "Authorization=Bearer raw-token header=Basic abc123 "
        "placeholder={{ secret:scope/key }} api_key=plain"
    )

    redacted = redact_text(text)

    assert "s3cr3t" not in redacted
    assert "topsecret" not in redacted
    assert "raw-token" not in redacted
    assert "abc123" not in redacted
    assert "{{ secret:scope/key }}" not in redacted
    assert "api_key=plain" not in redacted
    assert redacted.count("***REDACTED***") >= 6


def test_connector_metadata_redacts_sensitive_identifiers_and_paths(monkeypatch):
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
    spec = ConnectorSpec(
        connector="json",
        name="orders?token=source-name-secret",
        path="s3://bucket/orders?api_key=path-secret&status=open",
        options={"header": "Bearer option-secret"},
    )

    resolved = FileConnector("json").resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert "source-name-secret" not in resolved.label
    assert "path-secret" not in resolved.label
    _assert_text_not_present(resolved.metadata, "source-name-secret")
    _assert_text_not_present(resolved.metadata, "path-secret")
    _assert_text_not_present(resolved.metadata, "option-secret")
    assert calls["options"]["header"] == "Bearer option-secret"


def test_file_connector_uses_spark_reader(monkeypatch):
    calls = {"format": None, "options": {}, "schema": None, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            calls["options"].update(kwargs)
            return self

        def schema(self, value):
            calls["schema"] = value
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
    assert calls == {"format": "json", "options": {"multiline": "true"}, "schema": None, "load": "/landing/orders"}


def test_file_connector_applies_declared_schema_from_read(monkeypatch):
    calls = {"format": None, "options": {}, "schema": None, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            calls["options"].update(kwargs)
            return self

        def schema(self, value):
            calls["schema"] = value
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "csv",
            "path": "/landing/orders",
            "options": {"header": True, "schema": "should_not_be_forwarded STRING"},
            "read": {"schema": "order_id STRING, amount DOUBLE", "source_complete": True},
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls == {
        "format": "csv",
        "options": {"header": "true"},
        "schema": "order_id STRING, amount DOUBLE",
        "load": "/landing/orders",
    }
    assert resolved.metadata["source_metrics"]["schema_declared"] is True


def test_file_connector_accepts_top_level_source_schema_alias(monkeypatch):
    calls = {"format": None, "options": {}, "schema": None, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            calls["options"].update(kwargs)
            return self

        def schema(self, value):
            calls["schema"] = value
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "csv",
            "path": "/landing/orders",
            "schema": "order_id STRING, amount DOUBLE",
            "options": {"header": True},
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert plan.source.read["schema"] == "order_id STRING, amount DOUBLE"
    assert calls == {
        "format": "csv",
        "options": {"header": "true"},
        "schema": "order_id STRING, amount DOUBLE",
        "load": "/landing/orders",
    }
    assert resolved.metadata["source_metrics"]["schema_declared"] is True


def test_file_connector_applies_file_regex_filter(monkeypatch):
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
    monkeypatch.setattr(
        FileConnector,
        "_list_files",
        staticmethod(
            lambda path, recursive, max_files: [
                "/landing/orders/2026-05-01/orders_001.csv",
                "/landing/orders/2026-05-01/_SUCCESS",
                "/landing/orders/2026-05-02/orders_002.csv",
            ]
        ),
    )
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "csv",
            "path": "/landing/orders",
            "options": {"header": True, "recursiveFileLookup": True},
            "read": {
                "file_regex": r"orders_\d+\.csv$",
                "file_regex_scope": "filename",
                "file_regex_max_listed": 100,
            },
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["load"] == [
        "/landing/orders/2026-05-01/orders_001.csv",
        "/landing/orders/2026-05-02/orders_002.csv",
    ]
    assert calls["options"] == {"header": "true", "recursiveFileLookup": "true"}
    metrics = resolved.metadata["source_metrics"]
    assert metrics["file_regex_applied"] is True
    assert metrics["file_regex_scope"] == "filename"
    assert metrics["file_regex_recursive"] is True
    assert metrics["files_listed"] == 3
    assert metrics["files_matched"] == 2


def test_file_connector_file_regex_errors_when_no_match(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(
        FileConnector,
        "_list_files",
        staticmethod(lambda path, recursive, max_files: ["/landing/orders/_SUCCESS"]),
    )
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "csv",
            "path": "/landing/orders",
            "read": {"file_regex": r"orders_\d+\.csv$"},
        },
        target_table="b_orders",
    )

    with pytest.raises(ValueError, match="source.read.file_regex não encontrou arquivos"):
        resolve_batch_source(plan.source, plan)


def test_connector_rejects_conflicting_top_level_source_schema_alias():
    with pytest.raises(ValueError, match="source.schema conflita com source.read.schema"):
        build_plan_from_kwargs(
            source={
                "type": "connector",
                "connector": "csv",
                "path": "/landing/orders",
                "schema": "order_id STRING",
                "read": {"schema": "event_id STRING"},
            },
            target_table="b_orders",
        )


def test_object_storage_alias_sets_provider_and_uses_declared_format(monkeypatch):
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
        source={
            "type": "connector",
            "connector": "s3",
            "format": "parquet",
            "path": "s3://landing/orders",
            "read": {"source_complete": True},
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls == {"format": "parquet", "options": {}, "load": "s3://landing/orders"}
    assert resolved.metadata["source_provider"] == "s3"
    assert resolved.metadata["source_metrics"]["object_storage_provider"] == "s3"
    assert resolved.metadata["source_metrics"]["source_complete"] is True


def test_s3_connector_configures_static_credentials_from_auth(monkeypatch):
    calls = {"format": None, "options": {}, "load": None, "conf": {}}

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

    class Conf:
        def set(self, key, value):
            calls["conf"][key] = value

    class FakeSpark:
        read = Reader()
        conf = Conf()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setenv("CONTRACTFORGE_SECRET_AWS_ACCESS_KEY_ID", "AKIA_TEST")
    monkeypatch.setenv("CONTRACTFORGE_SECRET_AWS_SECRET_ACCESS_KEY", "SECRET_TEST")
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "s3",
            "format": "csv",
            "path": "s3a://landing/orders",
            "auth": {
                "access_key_id": "{{ secret:aws/access-key-id }}",
                "secret_access_key": "{{ secret:aws/secret-access-key }}",
            },
            "options": {"header": True, "fs.s3a.endpoint": "s3.us-east-1.amazonaws.com"},
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["format"] == "csv"
    assert calls["options"] == {"header": "true"}
    assert calls["load"] == "s3a://landing/orders"
    assert calls["conf"] == {
        "fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
        "fs.s3a.access.key": "AKIA_TEST",
        "fs.s3a.secret.key": "SECRET_TEST",
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }
    assert resolved.metadata["source_metrics"]["s3_auth_configured"] is True
    assert resolved.metadata["source_metrics"]["s3_temporary_credentials"] is False
    assert resolved.metadata["source_metrics"]["s3_conf_options_configured"] == 1
    _assert_text_not_present(resolved.metadata, "AKIA_TEST")
    _assert_text_not_present(resolved.metadata, "SECRET_TEST")


def test_s3_connector_configures_temporary_credentials_from_auth(monkeypatch):
    calls = {"conf": {}}

    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

        def load(self, path):
            return "df"

    class Conf:
        def set(self, key, value):
            calls["conf"][key] = value

    class FakeSpark:
        read = Reader()
        conf = Conf()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "s3",
            "format": "json",
            "path": "s3a://landing/events",
            "auth": {
                "access_key": "AKIA_TEST",
                "secret_key": "SECRET_TEST",
                "token": "SESSION_TEST",
            },
        },
        target_table="b_events",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["conf"]["fs.s3a.session.token"] == "SESSION_TEST"
    assert calls["conf"]["fs.s3a.aws.credentials.provider"] == (
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assert resolved.metadata["source_metrics"]["s3_auth_configured"] is True
    assert resolved.metadata["source_metrics"]["s3_temporary_credentials"] is True
    _assert_text_not_present(resolved.metadata, "SESSION_TEST")


def test_s3_connector_requires_access_and_secret_key_together(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "s3",
            "format": "csv",
            "path": "s3a://landing/orders",
            "auth": {"access_key_id": "AKIA_TEST"},
        },
        target_table="b_orders",
    )

    with pytest.raises(ValueError, match="access_key_id e secret_access_key"):
        resolve_batch_source(plan.source, plan)


def test_s3_connector_reports_clear_error_when_spark_conf_is_blocked(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

    class Conf:
        def set(self, key, value):
            raise RuntimeError("CONFIG_NOT_AVAILABLE: Configuration fs.s3a.access.key is not available")

    class FakeSpark:
        read = Reader()
        conf = Conf()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "s3",
            "format": "csv",
            "path": "s3a://landing/orders",
            "auth": {"access_key_id": "AKIA_TEST", "secret_access_key": "SECRET_TEST"},
        },
        target_table="b_orders",
    )

    with pytest.raises(RuntimeError, match="External Location"):
        resolve_batch_source(plan.source, plan)


@pytest.mark.parametrize("fmt", ["avro", "xml"])
def test_object_storage_accepts_avro_and_xml_formats(monkeypatch, fmt):
    calls = {"format": None, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "azure_blob",
            "format": fmt,
            "path": f"wasbs://container@account.blob.core.windows.net/datasets/{fmt}/sample",
        },
        target_table="b_data",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["format"] == fmt
    assert calls["load"].endswith(f"datasets/{fmt}/sample")


@pytest.mark.parametrize("fmt", ["jsonl", "ndjson"])
def test_file_and_object_storage_map_json_lines_to_spark_json(monkeypatch, fmt):
    calls = {"format": None, "load": None}

    class Reader:
        def format(self, value):
            calls["format"] = value
            return self

        def options(self, **kwargs):
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    plan = build_plan_from_kwargs(
        source={"type": "connector", "connector": "azure_blob", "format": fmt, "path": f"wasbs://c@a.blob.core.windows.net/data/events.{fmt}"},
        target_table="b_events",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["format"] == "json"
    assert calls["load"].endswith(f"events.{fmt}")


def test_azure_blob_connector_builds_wasbs_path_and_configures_sas(monkeypatch):
    calls = {"format": None, "options": {}, "load": None, "conf": {}}

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

    class Conf:
        def set(self, key, value):
            calls["conf"][key] = value

    class FakeSpark:
        read = Reader()
        conf = Conf()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setenv("CONTRACTFORGE_SECRET_SCOPE_BLOB_SAS", "?sv=1&sig=secret")
    plan = build_plan_from_kwargs(
        source={
            "type": "connector",
            "connector": "azure_blob",
            "format": "csv",
            "account_url": "https://exampleacct.blob.core.windows.net/",
            "container": "landing",
            "path": "datasets/csv/partitioned/sales",
            "auth": {"sas_token": "{{ secret:scope/blob-sas }}"},
            "options": {"header": True},
        },
        target_table="b_orders",
    )

    resolved = resolve_batch_source(plan.source, plan)

    assert resolved.df == "df"
    assert calls["format"] == "csv"
    assert calls["options"] == {"header": "true"}
    assert calls["load"] == (
        "wasbs://landing@exampleacct.blob.core.windows.net/datasets/csv/partitioned/sales"
    )
    assert calls["conf"] == {
        "fs.azure.sas.landing.exampleacct.blob.core.windows.net": "sv=1&sig=secret"
    }
    assert resolved.metadata["source_provider"] == "azure_blob"
    assert resolved.metadata["source_metrics"]["azure_auth_configured"] is True
    assert resolved.metadata["source_metrics"]["azure_container"] == "landing"
    _assert_text_not_present(resolved.metadata, "secret")


def test_azure_blob_connector_configures_sas_for_declared_wasbs_path(monkeypatch):
    calls = {"load": None, "conf": {}}

    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

        def load(self, path):
            calls["load"] = path
            return "df"

    class Conf:
        def set(self, key, value):
            calls["conf"][key] = value

    class FakeSpark:
        read = Reader()
        conf = Conf()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    spec = ConnectorSpec(
        connector="azure_blob",
        format="json",
        path="wasbs://landing@exampleacct.blob.core.windows.net/datasets/json/jsonl",
        auth={"sas_token": "sv=1&sig=secret"},
    )

    resolved = resolve_batch_source(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == "df"
    assert calls["load"] == "wasbs://landing@exampleacct.blob.core.windows.net/datasets/json/jsonl"
    assert calls["conf"] == {
        "fs.azure.sas.landing.exampleacct.blob.core.windows.net": "sv=1&sig=secret"
    }


def test_azure_blob_connector_reports_blocked_spark_config_with_serverless_guidance(monkeypatch):
    def blocked_config(account, container, sas_token):
        raise RuntimeError("[CONFIG_NOT_AVAILABLE] Configuration fs.azure.sas is not available")

    monkeypatch.setattr(ObjectStorageConnector, "_configure_azure_blob_sas", staticmethod(blocked_config))
    spec = ConnectorSpec(
        connector="azure_blob",
        format="csv",
        account_url="https://exampleacct.blob.core.windows.net/",
        container="landing",
        path="datasets/csv/large/orders_large.csv",
        auth={"sas_token": "sv=1&sig=secret"},
    )

    with pytest.raises(RuntimeError, match="External Location/Volume"):
        resolve_batch_source(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_http_csv_connector_downloads_csv_without_spark_https_filesystem(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, headers, timeout):
        assert url == "https://raw.example.com/orders.csv?region=br"
        assert headers["X-Trace"] == "unit"
        return b"id,name\n1,Ana\n2,Beto\n", {}, url, 21

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="http_file",
        format="csv",
        path="https://raw.example.com/orders.csv",
        request={"headers": {"X-Trace": "unit"}, "params": {"region": "br"}},
        options={"header": True},
        read={"source_complete": True},
    )

    resolved = HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df["rows"] == [{"id": "1", "name": "Ana"}, {"id": "2", "name": "Beto"}]
    assert resolved.metadata["source_format"] == "csv"
    assert resolved.metadata["source_metrics"]["read_strategy"] == "http_file"
    assert resolved.metadata["source_metrics"]["file_format"] == "csv"
    assert resolved.metadata["source_metrics"]["records_read"] == 2
    assert resolved.metadata["source_metrics"]["bytes_read"] == 21
    assert resolved.metadata["source_metrics"]["source_complete"] is True


def test_http_file_connector_supports_json_records_path(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, headers, timeout):
        return b'{"data":[{"id":1},{"id":2}]}', {}, url, 28

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="http_file",
        format="json",
        path="https://api.example.com/file.json",
        response={"records_path": "$.data"},
    )

    resolved = HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df["rows"] == [{"id": 1}, {"id": 2}]
    assert resolved.metadata["source_metrics"]["records_read"] == 2


def test_http_file_connector_supports_json_records_path_with_array_indexes(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, headers, timeout):
        return b'{"pages":[{"items":[{"id":1}]},{"items":[{"id":2},{"id":3}]}]}', {}, url, 63

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="http_file",
        format="json",
        path="https://api.example.com/file.json",
        response={"records_path": "$.pages[1].items"},
    )

    resolved = HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df["rows"] == [{"id": 2}, {"id": 3}]
    assert resolved.metadata["source_metrics"]["records_read"] == 2


def test_http_file_connector_supports_root_array_index_records_path(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, headers, timeout):
        return b'[[{"id":1}],[{"id":2},{"id":3}]]', {}, url, 33

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="http_file",
        format="json",
        path="https://api.example.com/file.json",
        response={"records_path": "$[1]"},
    )

    resolved = HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df["rows"] == [{"id": 2}, {"id": 3}]


def test_http_file_connector_rejects_index_on_non_array_records_path(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, headers, timeout):
        return b'{"data":{"items":[{"id":1}]}}', {}, url, 27

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="http_file",
        format="json",
        path="https://api.example.com/file.json",
        response={"records_path": "$.data[0]"},
    )

    with pytest.raises(ValueError, match="não é array"):
        HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_http_file_requires_declared_format():
    with pytest.raises(ValueError, match="source.format"):
        build_plan_from_kwargs(
            source={"type": "connector", "connector": "http_file", "path": "https://example.com/data"},
            target_table="b_data",
        )


def test_object_storage_alias_rejects_conflicting_provider():
    with pytest.raises(ValueError, match="conflita"):
        build_plan_from_kwargs(
            source={
                "type": "connector",
                "connector": "s3",
                "provider": "gcs",
                "format": "parquet",
                "path": "s3://landing/orders",
            },
            target_table="b_orders",
        )


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
        "jdbc_auth_configured": False,
        "jdbc_auth_type": "none",
    }


def test_jdbc_connector_extracts_typed_watermark_for_incremental_predicate(monkeypatch):
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
        watermark_columns=["updated_at"],
        runtime_parameters={
            "_contractforge_watermark_previous": (
                '{"updated_at": {"type": "timestamp", "value": "2026-05-16 12:00:00"}}'
            )
        },
    )

    resolved = JdbcConnector().resolve_batch(spec, plan)

    assert resolved.df == "df"
    assert captured["dbtable"] == (
        "(SELECT * FROM public.orders WHERE updated_at > '2026-05-16 12:00:00') cf_src"
    )
    assert resolved.metadata["source_metrics"]["incremental_applied"] is True
    assert resolved.metadata["source_metrics"]["watermark_value"] == "2026-05-16 12:00:00"


def test_jdbc_connector_rejects_typed_composite_watermark_without_incremental_column():
    spec = ConnectorSpec(
        connector="jdbc",
        options={"url": "jdbc:test", "dbtable": "public.orders"},
        incremental={"predicate": "updated_at > '{watermark}'"},
    )
    plan = build_plan_from_kwargs(
        source="x",
        target_table="t",
        watermark_columns=["updated_at", "sequence_id"],
        runtime_parameters={
            "_contractforge_watermark_previous": (
                '{"updated_at": {"type": "timestamp", "value": "2026-05-16 12:00:00"}, '
                '"sequence_id": {"type": "bigint", "value": "10"}}'
            )
        },
    )

    with pytest.raises(ValueError, match="watermark composto"):
        JdbcConnector().resolve_batch(spec, plan)


def test_named_jdbc_connector_uses_jdbc_reader(monkeypatch):
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
        connector="postgres",
        options={"url": "jdbc:postgresql://host/db", "dbtable": "public.orders"},
        read={"fetchsize": 5000},
    )

    resolved = JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == "df"
    assert resolved.label == "postgres:public.orders"
    assert captured["format"] == "jdbc"
    assert captured["url"] == "jdbc:postgresql://host/db"
    assert captured["dbtable"] == "public.orders"
    assert captured["fetchsize"] == "5000"
    assert resolved.metadata["source_connector"] == "postgres"


def test_jdbc_connector_applies_basic_auth_from_source_auth(monkeypatch):
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
        connector="postgres",
        options={"url": "jdbc:postgresql://host/db", "dbtable": "public.orders"},
        auth={"type": "basic", "username": "app_user", "password": "secret-password"},
    )

    resolved = JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == "df"
    assert captured["user"] == "app_user"
    assert captured["password"] == "secret-password"
    assert resolved.metadata["source_metrics"]["jdbc_auth_configured"] is True
    assert resolved.metadata["source_metrics"]["jdbc_auth_type"] == "basic"
    _assert_text_not_present(resolved.metadata, "secret-password")


def test_jdbc_connector_generates_rds_iam_auth_token(monkeypatch):
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
        connector="postgres",
        options={
            "url": "jdbc:postgresql://database-1.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:5432/postgres",
            "dbtable": "public.orders",
        },
        auth={
            "type": "rds_iam",
            "username": "postgres",
            "access_key_id": "AKIA_TEST",
            "secret_access_key": "SECRET_TEST",
            "session_token": "SESSION_TEST",
        },
    )

    resolved = JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == "df"
    assert captured["user"] == "postgres"
    assert captured["password"].startswith(
        "database-1.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:5432/?"
    )
    assert "Action=connect" in captured["password"]
    assert "DBUser=postgres" in captured["password"]
    assert "X-Amz-Security-Token=" in captured["password"]
    assert captured["ssl"] == "true"
    assert captured["sslmode"] == "require"
    assert resolved.metadata["source_metrics"]["jdbc_auth_configured"] is True
    assert resolved.metadata["source_metrics"]["jdbc_auth_type"] == "rds_iam"
    assert resolved.metadata["source_metrics"]["jdbc_rds_iam_token_generated"] is True
    assert resolved.metadata["source_metrics"]["jdbc_rds_region"] == "us-east-1"
    assert resolved.metadata["source_metrics"]["jdbc_rds_iam_credential_source"] == "explicit"
    _assert_text_not_present(resolved.metadata, "SECRET_TEST")
    _assert_text_not_present(resolved.metadata, "SESSION_TEST")


def test_jdbc_connector_generates_rds_iam_auth_token_with_default_chain(monkeypatch):
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
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setattr(
        sources_module,
        "_aws_credentials_from_default_chain",
        lambda: ("AKIA_CHAIN", "SECRET_CHAIN", "TOKEN_CHAIN"),
    )
    spec = ConnectorSpec(
        connector="postgres",
        options={
            "url": "jdbc:postgresql://database-1.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:5432/postgres",
            "dbtable": "public.orders",
        },
        auth={
            "type": "rds_iam",
            "username": "postgres",
            "credential_provider": "default_chain",
        },
    )

    resolved = JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == "df"
    assert captured["user"] == "postgres"
    assert "Action=connect" in captured["password"]
    assert "X-Amz-Security-Token=" in captured["password"]
    assert resolved.metadata["source_metrics"]["jdbc_rds_iam_credential_source"] == "default_chain"
    _assert_text_not_present(resolved.metadata, "SECRET_CHAIN")
    _assert_text_not_present(resolved.metadata, "TOKEN_CHAIN")


def test_jdbc_connector_generates_rds_iam_auth_token_with_mysql_default_port(monkeypatch):
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
        connector="mysql",
        options={
            "url": "jdbc:mysql://orders.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com/app",
            "dbtable": "orders",
        },
        auth={
            "type": "rds_iam",
            "username": "app_user",
            "access_key_id": "AKIA_TEST",
            "secret_access_key": "SECRET_TEST",
        },
    )

    JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert captured["password"].startswith(
        "orders.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:3306/?"
    )
    assert "DBUser=app_user" in captured["password"]


def test_jdbc_connector_rejects_unknown_rds_iam_credential_provider(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    spec = ConnectorSpec(
        connector="postgres",
        options={"url": "jdbc:postgresql://host.us-east-1.rds.amazonaws.com:5432/postgres", "dbtable": "public.orders"},
        auth={"type": "rds_iam", "username": "postgres", "credential_provider": "metadata_only"},
    )

    with pytest.raises(ValueError, match="credential_provider"):
        JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_jdbc_connector_rejects_rds_iam_without_aws_credentials(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    spec = ConnectorSpec(
        connector="postgres",
        options={"url": "jdbc:postgresql://host.us-east-1.rds.amazonaws.com:5432/postgres", "dbtable": "public.orders"},
        auth={"type": "rds_iam", "username": "postgres"},
    )

    with pytest.raises(ValueError, match="access_key_id e secret_access_key"):
        JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_jdbc_connector_metadata_never_exposes_credentials(monkeypatch):
    class Reader:
        def format(self, value):
            return self

        def options(self, **kwargs):
            return self

        def load(self):
            return "df"

    class FakeSpark:
        read = Reader()

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    spec = ConnectorSpec(
        connector="postgres",
        options={
            "url": "jdbc:postgresql://user:jdbc-secret@host/db?password=param-secret",
            "dbtable": "public.orders",
            "user": "plain-user",
            "password": "plain-password",
        },
    )

    resolved = JdbcConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    _assert_text_not_present(resolved.metadata, "jdbc-secret")
    _assert_text_not_present(resolved.metadata, "param-secret")
    _assert_text_not_present(resolved.metadata, "plain-password")


def test_spark_format_connector_uses_table_from_source(monkeypatch):
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
        connector="bigquery",
        table="project.dataset.orders",
        options={"api_key": "plain"},
    )

    resolved = SparkFormatConnector("bigquery", table_option="table").resolve_batch(
        spec,
        build_plan_from_kwargs(source="x", target_table="t"),
    )

    assert resolved.df == "df"
    assert captured["format"] == "bigquery"
    assert captured["table"] == "project.dataset.orders"
    assert resolved.metadata["source_metrics"]["spark_format"] == "bigquery"
    assert resolved.metadata["source_options_redacted"]["api_key"] == "***REDACTED***"


def test_diagnose_source_connectors_reports_runtime_requirements():
    diagnostics = diagnose_source_connectors(["rest_api", "snowflake", "s3"])
    by_name = {item["name"]: item for item in diagnostics}

    assert by_name["rest_api"]["status"] == "ok"
    assert "urllib" in by_name["rest_api"]["runtime"]
    assert by_name["snowflake"]["status"] == "runtime_required"
    assert "Snowflake" in by_name["snowflake"]["runtime"]
    assert by_name["s3"]["status"] == "runtime_required"
    assert "S3" in by_name["s3"]["runtime"]


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

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        requested_urls.append(url)
        payload, headers_payload = responses.pop(0)
        return payload, headers_payload, url, len(str(payload).encode("utf-8")), str(payload)

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

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        requested["url"] = url
        requested["headers"] = headers
        requested["body"] = body
        return {"data": [{"id": 1}]}, {}, url, 24, '{"data":[{"id":1}]}'

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


def test_rest_api_connector_materializes_complex_records_as_json_lines(monkeypatch):
    captured = {}

    class FakeContext:
        def parallelize(self, rows):
            captured["json_lines"] = rows
            return rows

    class FakeReader:
        def option(self, key, value):
            captured.setdefault("options", {})[key] = value
            return self

        def json(self, rows):
            return {"json_lines": rows}

    class FakeSpark:
        sparkContext = FakeContext()
        read = FakeReader()

        def createDataFrame(self, rows, schema=None):
            raise AssertionError("rest_api records should use spark.read.json when available")

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        payload = {
            "data": [
                {
                    "id": "R1",
                    "display_name": "Record",
                    "authorships": [{"author": {"id": "A1"}}, {"author": {"id": "A2"}, "raw": ["x"]}],
                    "open_access": {"is_oa": True},
                }
            ]
        }
        text = '{"data":[{"id":"R1"}]}'
        return payload, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        name="records_api",
        request={"url": "https://api.example.com/items"},
        response={"records_path": "$.data"},
        read={"json_options": {"readerCaseSensitive": True}},
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == {"json_lines": captured["json_lines"]}
    assert '"authorships"' in captured["json_lines"][0]
    assert captured["options"] == {"readerCaseSensitive": "true"}
    assert resolved.metadata["source_metrics"]["dataframe_materialization"] == "spark_read_json_lines"


def test_rest_api_connector_uses_configured_staging_when_spark_context_is_unavailable(monkeypatch, tmp_path):
    captured = {}

    class FakeReader:
        def schema(self, value):
            captured["schema"] = value
            return self

        def option(self, key, value):
            captured.setdefault("options", {})[key] = value
            return self

        def json(self, path):
            captured["path"] = path
            return {"json_path": path}

    class FakeSpark:
        read = FakeReader()

        def createDataFrame(self, rows, schema=None):
            raise AssertionError("rest_api records should use spark.read.json before createDataFrame fallback")

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        payload = {
            "data": [
                {
                    "id": "R1",
                    "quality_score": {"value": 99.1, "is_in_top_percentile": True},
                }
            ]
        }
        text = '{"data":[{"id":"R1"}]}'
        return payload, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        name="records_api",
        request={"url": "https://api.example.com/items"},
        response={"records_path": "$.data"},
        read={
            "staging_path": str(tmp_path),
            "schema": "id STRING, quality_score STRUCT<value:DOUBLE,is_in_top_percentile:BOOLEAN>",
            "json_options": {"rescuedDataColumn": "_rescued_data"},
        },
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == {"json_path": captured["path"]}
    assert str(captured["path"]).endswith(".jsonl")
    assert captured["schema"] == "id STRING, quality_score STRUCT<value:DOUBLE,is_in_top_percentile:BOOLEAN>"
    assert captured["options"] == {"rescuedDataColumn": "_rescued_data"}
    assert resolved.metadata["source_metrics"]["dataframe_materialization"] == "spark_read_json_staging_file"
    assert resolved.metadata["source_metrics"]["schema_declared"] is True


def test_http_file_connector_applies_declared_schema_to_parsed_records(monkeypatch, tmp_path):
    captured = {}

    class FakeReader:
        def schema(self, value):
            captured["schema"] = value
            return self

        def option(self, key, value):
            captured.setdefault("options", {})[key] = value
            return self

        def json(self, path):
            captured["path"] = path
            return {"json_path": path}

    class FakeSpark:
        read = FakeReader()

        def createDataFrame(self, rows, schema=None):
            raise AssertionError("http_file parsed records should use spark.read.json when staging is declared")

    def fake_request_with_retry(self, url, headers, timeout, retry_attempts, backoff):
        return b"id,amount\nA1,10.5\n", {}, url, 18

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(HttpFileConnector, "_request_with_retry", fake_request_with_retry)
    spec = ConnectorSpec(
        connector="http_file",
        name="orders_csv",
        format="csv",
        request={"url": "https://api.example.com/orders.csv"},
        read={"staging_path": str(tmp_path), "schema": "id STRING, amount DOUBLE"},
    )

    resolved = HttpFileConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert resolved.df == {"json_path": captured["path"]}
    assert captured["schema"] == "id STRING, amount DOUBLE"
    assert resolved.metadata["source_metrics"]["schema_declared"] is True


def test_rest_api_connector_requires_staging_for_complex_records_without_spark_context(monkeypatch):
    class FakeSpark:
        class FakeReader:
            def json(self, path):
                return {"json_path": path}

        read = FakeReader()

        def createDataFrame(self, rows, schema=None):
            raise TypeError("cannot infer nested field")

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        payload = {"data": [{"id": "R1", "nested": {"value": 1}}]}
        text = '{"data":[{"id":"R1"}]}'
        return payload, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        request={"url": "https://api.example.com/items"},
        response={"records_path": "$.data"},
    )

    with pytest.raises(ValueError, match="source.read.staging_path"):
        RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_rest_api_connector_can_return_raw_payload_without_json_parsing(monkeypatch):
    requested = {}

    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        requested["parse_json_payload"] = parse_json_payload
        text = '{"events":[{"id":"E1","geometry":[{"date":"2026-05-15"}]}]}'
        return None, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        name="events_api",
        request={"url": "https://api.example.com/events"},
        response={"mode": "raw", "raw_column": "raw_response"},
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert requested["parse_json_payload"] is False
    assert resolved.df["rows"] == [
        {
            "raw_response": '{"events":[{"id":"E1","geometry":[{"date":"2026-05-15"}]}]}',
            "response_page_number": 1,
        }
    ]
    assert resolved.metadata["source_metrics"]["response_mode"] == "raw"
    assert resolved.metadata["source_metrics"]["records_read"] == 1
    assert resolved.metadata["source_metrics"]["raw_payloads_read"] == 1


def test_rest_api_raw_mode_preserves_cursor_pagination(monkeypatch):
    responses = [
        ({"next": "abc"}, '{"data":[{"id":1}],"next":"abc"}'),
        ({"next": None}, '{"data":[{"id":2}],"next":null}'),
    ]
    requested_parse_modes = []

    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        requested_parse_modes.append(parse_json_payload)
        payload, text = responses.pop(0)
        return payload, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        request={"url": "https://api.example.com/events"},
        pagination={"type": "cursor", "cursor_param": "cursor", "next_cursor_path": "$.next"},
        response={"mode": "raw"},
        limits={"max_pages": 2},
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    assert requested_parse_modes == [True, True]
    assert [row["response_page_number"] for row in resolved.df["rows"]] == [1, 2]
    assert [row["raw_response"] for row in resolved.df["rows"]] == [
        '{"data":[{"id":1}],"next":"abc"}',
        '{"data":[{"id":2}],"next":null}',
    ]
    assert resolved.metadata["source_metrics"]["request_count"] == 2


def test_rest_api_connector_rejects_payload_over_byte_limits(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        text = '{"data":[]}'
        return {"data": []}, {}, url, len(text.encode("utf-8")), text

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        request={"url": "https://api.example.com/events"},
        response={"mode": "raw"},
        limits={"max_page_bytes": 3},
    )

    with pytest.raises(ValueError, match="max_page_bytes"):
        RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))


def test_rest_api_raw_mode_contract_validation():
    with pytest.raises(ValueError, match="records_path"):
        build_plan_from_kwargs(
            source={
                "type": "connector",
                "connector": "rest_api",
                "request": {"url": "https://api.example.com/events"},
                "response": {"mode": "raw", "records_path": "$.data"},
            },
            target_table="b_events",
        )

    with pytest.raises(ValueError, match="response.mode"):
        build_plan_from_kwargs(
            source={
                "type": "connector",
                "connector": "rest_api",
                "request": {"url": "https://api.example.com/events"},
                "response": {"mode": "invalid"},
            },
            target_table="b_events",
        )


def test_rest_api_connector_validates_missing_bearer_token():
    spec = ConnectorSpec(
        connector="rest_api",
        request={"url": "https://api.example.com/orders"},
        auth={"type": "bearer_token"},
    )

    with pytest.raises(ValueError, match="auth.token"):
        RestApiConnector()._headers(spec)


def test_rest_api_connector_metadata_never_exposes_auth_or_request_secrets(monkeypatch):
    class FakeSpark:
        def createDataFrame(self, rows, schema=None):
            return {"rows": rows, "schema": schema}

    def fake_request(self, url, method, headers, body, timeout, *, parse_json_payload=True):
        return {"data": [{"id": 1}]}, {}, url, 24, '{"data":[{"id":1}]}'

    monkeypatch.setattr(sources_module, "spark", FakeSpark())
    monkeypatch.setattr(RestApiConnector, "_request", fake_request)
    spec = ConnectorSpec(
        connector="rest_api",
        request={
            "url": "https://api.example.com/orders?api_key=query-secret",
            "headers": {"X-Api-Key": "header-secret"},
            "json": {"client_secret": "body-secret"},
        },
        auth={"type": "bearer_token", "token": "bearer-secret"},
        response={"records_path": "$.data"},
    )

    resolved = RestApiConnector().resolve_batch(spec, build_plan_from_kwargs(source="x", target_table="t"))

    _assert_text_not_present(resolved.metadata, "query-secret")
    _assert_text_not_present(resolved.metadata, "header-secret")
    _assert_text_not_present(resolved.metadata, "body-secret")
    _assert_text_not_present(resolved.metadata, "bearer-secret")


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

    def fake_stream(inner_plan, *, raise_on_failure=True):
        return {
            "status": "DRY_RUN",
            "stream_run_id": "stream-1",
            "source": inner_plan.source.path,
            "checkpoint": inner_plan.source.checkpoint_location,
            "raise_on_failure": raise_on_failure,
        }

    monkeypatch.setattr("contractforge.ingestion.ingest_stream_plan", fake_stream)

    assert ingest_plan(plan) == {
        "status": "DRY_RUN",
        "stream_run_id": "stream-1",
        "source": "/landing/orders",
        "checkpoint": "/checkpoints/orders",
        "raise_on_failure": True,
    }
