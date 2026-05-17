"""Templates de contratos para cenários comuns de uso."""
from __future__ import annotations

from copy import deepcopy
import json
from typing import Any, Dict


ContractTemplate = Dict[str, Any]

_TEMPLATE_META_KEY = "_template"


def _target(schema: str, table: str, *, catalog: str = "main") -> dict[str, str]:
    return {"catalog": catalog, "schema": schema, "table": table}


BUILTIN_CONTRACT_TEMPLATES: dict[str, ContractTemplate] = {
    "bronze_rest_api_incremental": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_rest_api_incremental",
            "description": "Bronze append incremental a partir de API REST paginada.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_file_append",
            "source": {
                "type": "connector",
                "connector": "rest_api",
                "name": "orders_api",
                "request": {
                    "url": "https://api.example.com/orders",
                    "params": {"status": "open"},
                },
                "auth": {"type": "bearer_token", "token": "{{ secret:orders_api/token }}"},
                "pagination": {
                    "type": "cursor",
                    "cursor_param": "cursor",
                    "next_cursor_path": "$.next",
                },
                "response": {"records_path": "$.data"},
                "incremental": {
                    "watermark_param": "updated_after",
                    "watermark_header": "X-Watermark",
                    "initial_value": "1970-01-01T00:00:00Z",
                },
                "limits": {"max_pages": 100, "timeout_seconds": 60, "retry_attempts": 3},
            },
            "target": _target("bronze", "b_orders_api"),
            "layer": "bronze",
            "mode": "scd0_append",
            "watermark_columns": ["updated_at"],
            "schema_policy": "additive_only",
            "quality_rules": {
                "not_null": ["id"],
                "expressions": [
                    {
                        "name": "valid_updated_at",
                        "expression": "updated_at IS NOT NULL",
                        "severity": "warn",
                        "message": "updated_at ausente no payload da API",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("bronze", "b_orders_api"),
            "table": {
                "description": "Pedidos recebidos da API externa em formato bronze.",
                "tags": {"domain": "sales", "source": "rest_api"},
            },
            "columns": {
                "id": {"description": "Identificador do pedido na API."},
                "updated_at": {"description": "Timestamp de atualização usado como watermark."},
            },
        },
        "operations": {
            "target": _target("bronze", "b_orders_api"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 120,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_orders_api",
            },
        },
    },
    "bronze_http_file_csv_snapshot": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_http_file_csv_snapshot",
            "description": "Bronze overwrite para CSV público/autenticado via HTTP(S).",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_full_overwrite",
            "source": {
                "type": "connector",
                "connector": "http_file",
                "path": "https://example.com/public/orders.csv",
                "format": "csv",
                "options": {"header": True, "multiLine": False},
                "read": {
                    "source_complete": True,
                    "schema": "order_id STRING, order_date DATE, customer_id STRING, amount DOUBLE, updated_at TIMESTAMP",
                    "timeout_seconds": 120,
                },
            },
            "target": _target("bronze", "b_http_orders_csv"),
            "layer": "bronze",
            "schema_policy": "additive_only",
            "quality_rules": {
                "not_null": ["order_id"],
                "expressions": [
                    {
                        "name": "valid_amount",
                        "expression": "amount IS NULL OR amount >= 0",
                        "severity": "warn",
                        "message": "amount negativo no CSV HTTP",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("bronze", "b_http_orders_csv"),
            "table": {
                "description": "Snapshot bronze de arquivo CSV lido por HTTP(S) no driver.",
                "tags": {"domain": "sales", "source": "http_file", "format": "csv"},
            },
            "columns": {
                "order_id": {"description": "Identificador do pedido no arquivo."},
                "updated_at": {"description": "Timestamp informado pela origem HTTP."},
            },
        },
        "operations": {
            "target": _target("bronze", "b_http_orders_csv"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "low",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 1440,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_http_orders_csv",
            },
        },
    },
    "bronze_object_storage_nested_json_shape": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_object_storage_nested_json_shape",
            "description": "Bronze/Silver-ready para JSON aninhado em object storage usando transform.shape.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_file_append",
            "source": {
                "type": "connector",
                "connector": "s3",
                "format": "json",
                "path": "s3a://company-landing/events/earthquakes/",
                "read": {
                    "source_complete": True,
                    "schema": (
                        "id STRING, "
                        "properties STRUCT<mag:DOUBLE,place:STRING,time:BIGINT,type:STRING,status:STRING>, "
                        "geometry STRUCT<type:STRING,coordinates:ARRAY<DOUBLE>>"
                    ),
                },
            },
            "target": _target("bronze", "b_earthquake_events"),
            "layer": "bronze",
            "mode": "scd0_append",
            "transform": {
                "shape": {
                    "columns": {
                        "id": "event_id",
                        "properties.mag": {"alias": "magnitude", "cast": "DOUBLE"},
                        "properties.place": "place",
                        "properties.time": {
                            "alias": "event_time",
                            "expression": "CAST(properties.time / 1000 AS TIMESTAMP)",
                        },
                        "properties.type": "event_type",
                        "properties.status": "status",
                        "geometry.type": "geometry_type",
                        "longitude_expr": {"alias": "longitude", "expression": "element_at(geometry.coordinates, 1)"},
                        "latitude_expr": {"alias": "latitude", "expression": "element_at(geometry.coordinates, 2)"},
                        "depth_expr": {"alias": "depth_km", "expression": "element_at(geometry.coordinates, 3)"},
                    }
                }
            },
            "quality_rules": {
                "not_null": ["event_id"],
                "expressions": [
                    {
                        "name": "valid_coordinates",
                        "expression": "longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90",
                        "severity": "quarantine",
                        "message": "Coordenadas fora do intervalo esperado",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("bronze", "b_earthquake_events"),
            "table": {
                "description": "Eventos geográficos normalizados de JSON aninhado em object storage.",
                "tags": {"domain": "geospatial", "source": "s3", "shape": "nested_json"},
            },
            "columns": {
                "event_id": {"description": "Identificador do evento."},
                "magnitude": {"description": "Magnitude do evento."},
                "event_time": {"description": "Timestamp convertido de epoch milliseconds."},
            },
        },
        "operations": {
            "target": _target("bronze", "b_earthquake_events"),
            "ownership": {
                "business_owner": "risk-analytics",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 180,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_earthquake_events",
            },
        },
    },
    "bronze_object_storage_small_files": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_object_storage_small_files",
            "description": "Bronze para muitas dezenas/centenas de arquivos pequenos com schema explícito.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_file_append",
            "source": {
                "type": "connector",
                "connector": "s3",
                "format": "csv",
                "path": "s3a://company-landing/small_files/orders/",
                "options": {
                    "header": True,
                    "recursiveFileLookup": True,
                    "pathGlobFilter": "*.csv",
                },
                "read": {
                    "source_complete": True,
                    "schema": "order_id STRING, event_date DATE, customer_id STRING, amount DOUBLE, file_batch STRING",
                    "file_regex": "^year=2026/month=05/day=\\d{2}/part-.*\\.csv$",
                    "file_regex_scope": "relative_path",
                    "file_regex_recursive": True,
                    "file_regex_max_listed": 50000,
                },
            },
            "target": _target("bronze", "b_orders_small_files"),
            "layer": "bronze",
            "mode": "scd0_append",
            "schema_policy": "additive_only",
            "quality_rules": {"not_null": ["order_id", "event_date"]},
        },
        "annotations": {
            "target": _target("bronze", "b_orders_small_files"),
            "table": {
                "description": "Carga bronze de muitos arquivos pequenos em object storage.",
                "tags": {"domain": "sales", "source": "object_storage", "pattern": "small_files"},
            },
            "columns": {
                "order_id": {"description": "Identificador do pedido."},
                "file_batch": {"description": "Identificador lógico do lote no arquivo."},
            },
        },
        "operations": {
            "target": _target("bronze", "b_orders_small_files"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 360,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_orders_small_files",
            },
        },
    },
    "bronze_autoloader_available_now_json": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_autoloader_available_now_json",
            "description": "Bronze Auto Loader available_now com checkpoint externo e microbatches.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_autoloader_append",
            "source": {
                "type": "autoloader",
                "format": "json",
                "path": "/Volumes/main/landing/orders_json",
                "schema_location": "/Volumes/main/ops/autoloader_schemas/orders_json",
                "checkpoint_location": "/Volumes/main/ops/checkpoints/orders_json",
                "max_files_per_trigger": 50,
                "include_existing_files": True,
                "schema_hints": "order_id STRING, updated_at TIMESTAMP",
            },
            "target_table": "b_orders_autoloader_json",
            "target": _target("bronze", "b_orders_autoloader_json"),
            "layer": "bronze",
            "idempotency_key": "b_orders_autoloader_json_available_now",
            "idempotency_policy": "skip_if_success",
            "quality_rules": {"not_null": ["order_id"]},
        },
        "annotations": {
            "target": _target("bronze", "b_orders_autoloader_json"),
            "table": {
                "description": "Arquivos JSON de pedidos ingeridos por Auto Loader em available_now.",
                "tags": {"domain": "sales", "source": "autoloader", "trigger": "available_now"},
            },
            "columns": {"order_id": {"description": "Chave do pedido no arquivo."}},
        },
        "operations": {
            "target": _target("bronze", "b_orders_autoloader_json"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 120,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_orders_autoloader_json",
            },
        },
    },
    "bronze_autoloader_json": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_autoloader_json",
            "description": "Bronze com Auto Loader JSON em available_now.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_autoloader_append",
            "source": {
                "type": "autoloader",
                "format": "json",
                "path": "/Volumes/main/landing/orders",
                "schema_location": "/Volumes/main/ops/schemas/orders",
                "checkpoint_location": "/Volumes/main/ops/checkpoints/orders",
            },
            "target_table": "b_orders_files",
            "target": _target("bronze", "b_orders_files"),
            "layer": "bronze",
            "idempotency_key": "b_orders_files_{{dt}}",
            "schema_policy": "additive_only",
            "quality_rules": {"not_null": ["_metadata.file_path"]},
        },
        "annotations": {
            "target": _target("bronze", "b_orders_files"),
            "table": {
                "description": "Arquivos JSON de pedidos ingeridos por Auto Loader.",
                "tags": {"domain": "sales", "source": "autoloader"},
            },
            "columns": {},
        },
        "operations": {
            "target": _target("bronze", "b_orders_files"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 120,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_orders_files",
            },
        },
    },
    "bronze_blob_partitioned_files": {
        _TEMPLATE_META_KEY: {
            "name": "bronze_blob_partitioned_files",
            "description": "Bronze batch para CSV/Parquet particionado em object storage.",
            "category": "bronze",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "bronze_file_append",
            "source": {
                "type": "connector",
                "connector": "s3",
                "format": "parquet",
                "path": "s3a://company-landing/orders/",
                "options": {
                    "recursiveFileLookup": True,
                    "pathGlobFilter": "*.parquet",
                },
                "read": {
                    "source_complete": True,
                    "schema": "order_id STRING, order_date DATE, customer_id STRING, amount DOUBLE",
                    "file_regex": "^year=2026/month=05/.*/orders_\\d+\\.parquet$",
                    "file_regex_scope": "relative_path",
                    "file_regex_max_listed": 50000,
                },
            },
            "target": _target("bronze", "b_orders_files"),
            "layer": "bronze",
            "mode": "scd0_append",
            "schema_policy": "additive_only",
            "quality_rules": {
                "not_null": ["order_id"],
                "expressions": [
                    {
                        "name": "valid_amount",
                        "expression": "amount IS NULL OR amount >= 0",
                        "severity": "warn",
                        "message": "amount negativo no arquivo bruto",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("bronze", "b_orders_files"),
            "table": {
                "description": "Arquivos particionados de pedidos em object storage.",
                "tags": {"domain": "sales", "source": "object_storage", "format": "parquet"},
            },
            "columns": {
                "order_id": {"description": "Identificador do pedido no arquivo."},
                "order_date": {"description": "Data do pedido usada para particionamento lógico."},
            },
        },
        "operations": {
            "target": _target("bronze", "b_orders_files"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 240,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/b_orders_files",
            },
        },
    },
    "silver_jdbc_scd1_upsert": {
        _TEMPLATE_META_KEY: {
            "name": "silver_jdbc_scd1_upsert",
            "description": "Silver SCD1 incremental a partir de JDBC.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations", "access"],
        },
        "ingestion": {
            "preset": ["silver_incremental_watermark_upsert", "quality_quarantine", "delta_optimized_writes"],
            "source": {
                "type": "connector",
                "connector": "postgres",
                "options": {
                    "url": "{{ secret:erp/postgres_url }}",
                    "dbtable": "public.orders",
                },
                "auth": {
                    "type": "basic",
                    "username": "{{ secret:erp/user }}",
                    "password": "{{ secret:erp/password }}",
                },
                "incremental": {
                    "watermark_column": "updated_at",
                    "initial_value": "1970-01-01 00:00:00",
                },
                "read": {
                    "fetchsize": 10000,
                    "partition_column": "id",
                    "lower_bound": 1,
                    "upper_bound": 10000000,
                    "num_partitions": 16,
                },
            },
            "target": _target("sales_curated", "s_orders"),
            "layer": "silver",
            "merge_keys": ["order_id"],
            "watermark_columns": ["updated_at"],
            "dedup_order_expr": "updated_at DESC NULLS LAST",
            "column_mapping": {"id": "order_id"},
            "quality_rules": {
                "not_null": ["order_id", "updated_at"],
                "unique_key": ["order_id"],
                "expressions": [
                    {
                        "name": "positive_amount",
                        "expression": "amount >= 0",
                        "severity": "quarantine",
                        "message": "amount negativo",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("sales_curated", "s_orders"),
            "table": {
                "description": "Pedidos consolidados em estado atual.",
                "tags": {"domain": "sales", "layer": "silver"},
            },
            "columns": {
                "order_id": {"description": "Chave do pedido."},
                "customer_email": {
                    "description": "Email do cliente.",
                    "pii": {"enabled": True, "type": "email", "sensitivity": "restricted"},
                    "tags": {"confidentiality": "restricted"},
                },
            },
        },
        "operations": {
            "target": _target("sales_curated", "s_orders"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "high",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 180,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_orders",
            },
        },
        "access": {
            "target": _target("sales_curated", "s_orders"),
            "access_policy": {"mode": "validate_only", "on_drift": "warn", "revoke_unmanaged": False},
            "grants": [{"principal": "sales-analysts", "privileges": ["SELECT"]}],
        },
    },
    "silver_jdbc_rds_iam_hash_diff": {
        _TEMPLATE_META_KEY: {
            "name": "silver_jdbc_rds_iam_hash_diff",
            "description": "Silver hash diff incremental a partir de Amazon RDS/Aurora com IAM auth.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations", "access"],
        },
        "ingestion": {
            "preset": ["silver_hash_diff_append", "quality_quarantine"],
            "source": {
                "type": "connector",
                "connector": "postgres",
                "options": {
                    "url": "jdbc:postgresql://orders.cluster-xyz.us-east-1.rds.amazonaws.com:5432/app",
                    "dbtable": "public.orders",
                    "driver": "org.postgresql.Driver",
                },
                "auth": {
                    "type": "rds_iam",
                    "username": "{{ secret:aws-rds/db_user }}",
                    "region": "us-east-1",
                    "credential_provider": "default_chain",
                },
                "incremental": {
                    "watermark_column": "updated_at",
                    "initial_value": "1970-01-01 00:00:00",
                },
                "read": {
                    "fetchsize": 10000,
                    "partition_column": "id",
                    "lower_bound": 1,
                    "upper_bound": 10000000,
                    "num_partitions": 8,
                },
            },
            "target": _target("sales_curated", "s_orders_hash_diff"),
            "layer": "silver",
            "hash_keys": ["order_id"],
            "watermark_columns": ["updated_at"],
            "hash_exclude_columns": ["updated_at", "ingestion_date", "ingestion_ts_utc", "__run_id"],
            "transform": {
                "deduplicate": {
                    "keys": ["order_id"],
                    "order_by": "updated_at DESC NULLS LAST, id DESC",
                }
            },
            "quality_rules": {
                "not_null": ["order_id", "updated_at"],
                "expressions": [
                    {
                        "name": "positive_amount",
                        "expression": "amount IS NULL OR amount >= 0",
                        "severity": "quarantine",
                        "message": "amount negativo no RDS",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("sales_curated", "s_orders_hash_diff"),
            "table": {
                "description": "Mudanças de pedidos vindas de RDS/Aurora detectadas por hash diff.",
                "tags": {"domain": "sales", "source": "rds_iam", "pattern": "hash_diff"},
            },
            "columns": {
                "order_id": {"description": "Chave natural do pedido."},
                "updated_at": {"description": "Watermark incremental da origem RDS/Aurora."},
            },
        },
        "operations": {
            "target": _target("sales_curated", "s_orders_hash_diff"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "high",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 120,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_orders_hash_diff",
            },
        },
        "access": {
            "target": _target("sales_curated", "s_orders_hash_diff"),
            "access_policy": {"mode": "validate_only", "on_drift": "warn", "revoke_unmanaged": False},
            "grants": [{"principal": "sales-analysts", "privileges": ["SELECT"]}],
        },
    },
    "silver_raw_json_payload_shape": {
        _TEMPLATE_META_KEY: {
            "name": "silver_raw_json_payload_shape",
            "description": "Silver normalizando coluna JSON string com transform.shape.parse_json.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "silver_scd1_upsert",
            "source": "bronze.b_api_raw_payloads",
            "target": _target("events_curated", "s_events"),
            "layer": "silver",
            "merge_keys": ["event_id"],
            "transform": {
                "shape": {
                    "parse_json": [
                        {
                            "column": "raw_payload",
                            "alias": "payload",
                            "schema": (
                                "STRUCT<event_id:STRING,source:STRING,occurred_at:STRING,"
                                "customer:STRUCT<id:STRING,email:STRING>,"
                                "metrics:STRUCT<amount:DOUBLE,currency:STRING>>"
                            ),
                            "drop_source": True,
                        }
                    ],
                    "columns": {
                        "payload.event_id": "event_id",
                        "payload.source": "event_source",
                        "payload.occurred_at": {
                            "alias": "occurred_at",
                            "expression": "CAST(payload.occurred_at AS TIMESTAMP)",
                        },
                        "payload.customer.id": "customer_id",
                        "payload.customer.email": "customer_email",
                        "payload.metrics.amount": {"alias": "amount", "cast": "DOUBLE"},
                        "payload.metrics.currency": "currency",
                    },
                }
            },
            "quality_rules": {
                "not_null": ["event_id", "occurred_at"],
                "unique_key": ["event_id"],
                "expressions": [
                    {
                        "name": "positive_amount",
                        "expression": "amount IS NULL OR amount >= 0",
                        "severity": "quarantine",
                        "message": "amount negativo no payload JSON",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("events_curated", "s_events"),
            "table": {
                "description": "Eventos normalizados a partir de coluna JSON string.",
                "tags": {"domain": "events", "shape": "parse_json"},
            },
            "columns": {
                "event_id": {"description": "Chave do evento."},
                "customer_email": {
                    "description": "Email do cliente extraído do payload.",
                    "pii": {"enabled": True, "type": "email", "sensitivity": "restricted"},
                    "tags": {"confidentiality": "restricted"},
                },
            },
        },
        "operations": {
            "target": _target("events_curated", "s_events"),
            "ownership": {
                "business_owner": "digital-products",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 120,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_events",
            },
        },
    },
    "silver_parallel_arrays_shape": {
        _TEMPLATE_META_KEY: {
            "name": "silver_parallel_arrays_shape",
            "description": "Silver para APIs com arrays paralelos usando zip_arrays + explode_outer.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "silver_scd1_upsert",
            "source": "bronze.b_openmeteo_forecast",
            "target": _target("weather_curated", "s_hourly_forecast"),
            "layer": "silver",
            "merge_keys": ["location_id", "forecast_hour"],
            "transform": {
                "shape": {
                    "zip_arrays": [
                        {
                            "alias": "hourly_rows",
                            "columns": {
                                "hourly.time": "time",
                                "hourly.temperature_2m": "temperature_2m",
                                "hourly.relative_humidity_2m": "relative_humidity_2m",
                            },
                        }
                    ],
                    "arrays": [{"path": "hourly_rows", "mode": "explode_outer", "alias": "hour"}],
                    "columns": {
                        "location_id": "location_id",
                        "hour.time": {"alias": "forecast_hour", "cast": "TIMESTAMP"},
                        "hour.temperature_2m": {"alias": "temperature_2m", "cast": "DOUBLE"},
                        "hour.relative_humidity_2m": {"alias": "relative_humidity_2m", "cast": "DOUBLE"},
                        "forecast_date_expr": {"alias": "forecast_date", "expression": "TO_DATE(hour.time)"},
                    },
                }
            },
            "quality_rules": {
                "not_null": ["location_id", "forecast_hour"],
                "unique_key": ["location_id", "forecast_hour"],
                "expressions": [
                    {
                        "name": "valid_humidity",
                        "expression": "relative_humidity_2m IS NULL OR relative_humidity_2m BETWEEN 0 AND 100",
                        "severity": "quarantine",
                        "message": "Umidade fora do intervalo 0-100",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("weather_curated", "s_hourly_forecast"),
            "table": {
                "description": "Previsão horária normalizada a partir de arrays paralelos de API.",
                "tags": {"domain": "weather", "shape": "zip_arrays"},
            },
            "columns": {
                "location_id": {"description": "Identificador lógico da localidade."},
                "forecast_hour": {"description": "Hora da previsão."},
            },
        },
        "operations": {
            "target": _target("weather_curated", "s_hourly_forecast"),
            "ownership": {
                "business_owner": "operations-analytics",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "hourly",
                "freshness_sla_minutes": 180,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_hourly_forecast",
            },
        },
    },
    "silver_snapshot_soft_delete": {
        _TEMPLATE_META_KEY: {
            "name": "silver_snapshot_soft_delete",
            "description": "Silver snapshot completo com marcação de ausentes como inativos.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "silver_snapshot_soft_delete",
            "source": {
                "type": "connector",
                "connector": "table",
                "table": "main.raw.devices_snapshot",
                "read": {"source_complete": True},
            },
            "target": _target("iot_curated", "s_devices"),
            "layer": "silver",
            "merge_keys": ["device_id"],
            "quality_rules": {
                "not_null": ["device_id"],
                "unique_key": ["device_id"],
            },
        },
        "annotations": {
            "target": _target("iot_curated", "s_devices"),
            "table": {
                "description": "Estado atual de dispositivos com soft delete.",
                "tags": {"domain": "iot", "pattern": "snapshot_soft_delete"},
            },
            "columns": {"device_id": {"description": "Identificador único do dispositivo."}},
        },
        "operations": {
            "target": _target("iot_curated", "s_devices"),
            "ownership": {
                "business_owner": "iot-ops",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "high",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 240,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_devices",
            },
        },
    },
    "silver_scd1_hash_diff": {
        _TEMPLATE_META_KEY: {
            "name": "silver_scd1_hash_diff",
            "description": "Silver append-only com hash diff para manter versões alteradas.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations"],
            "recommendation_priority": 10,
        },
        "ingestion": {
            "preset": "silver_hash_diff_append",
            "source": "bronze.b_products",
            "target": _target("catalog_curated", "s_products_hash_diff"),
            "layer": "silver",
            "mode": "scd1_hash_diff",
            "hash_keys": ["product_id"],
            "hash_exclude_columns": ["updated_at", "ingestion_date", "ingestion_ts_utc", "__run_id"],
            "transform": {
                "deduplicate": {
                    "keys": ["product_id"],
                    "order_by": "updated_at DESC NULLS LAST, ingestion_ts_utc DESC NULLS LAST",
                }
            },
            "quality_rules": {
                "not_null": ["product_id"],
                "expressions": [
                    {
                        "name": "valid_product_status",
                        "expression": "status IS NULL OR status IN ('active', 'inactive', 'discontinued')",
                        "severity": "quarantine",
                        "message": "status de produto inválido",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("catalog_curated", "s_products_hash_diff"),
            "table": {
                "description": "Versões alteradas de produtos detectadas por hash diff.",
                "tags": {"domain": "catalog", "pattern": "scd1_hash_diff"},
            },
            "columns": {
                "product_id": {"description": "Chave natural do produto."},
                "row_hash": {"description": "Hash técnico calculado pelo ContractForge."},
            },
        },
        "operations": {
            "target": _target("catalog_curated", "s_products_hash_diff"),
            "ownership": {
                "business_owner": "catalog",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "medium",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 240,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_products_hash_diff",
            },
        },
    },
    "silver_scd2_history": {
        _TEMPLATE_META_KEY: {
            "name": "silver_scd2_history",
            "description": "Silver histórico SCD2 para dimensões mutáveis.",
            "category": "silver",
            "files": ["ingestion", "annotations", "operations"],
        },
        "ingestion": {
            "preset": "silver_scd2_historical",
            "source": "bronze.b_customers",
            "target": _target("crm_curated", "s_customers_history"),
            "layer": "silver",
            "merge_keys": ["customer_id"],
            "dedup_order_expr": "updated_at DESC NULLS LAST",
            "hash_exclude_columns": ["updated_at", "ingestion_date", "ingestion_ts_utc", "__run_id"],
            "quality_rules": {
                "not_null": ["customer_id"],
                "expressions": [
                    {
                        "name": "valid_period",
                        "expression": "updated_at IS NOT NULL",
                        "severity": "abort",
                        "message": "updated_at obrigatório para histórico SCD2",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("crm_curated", "s_customers_history"),
            "table": {
                "description": "Histórico SCD2 de clientes.",
                "tags": {"domain": "crm", "history": "scd2"},
            },
            "columns": {
                "customer_id": {"description": "Chave do cliente."},
                "email": {
                    "description": "Email do cliente.",
                    "pii": {"enabled": True, "type": "email", "sensitivity": "restricted"},
                },
            },
        },
        "operations": {
            "target": _target("crm_curated", "s_customers_history"),
            "ownership": {
                "business_owner": "crm",
                "technical_owner": "data-platform",
                "support_group": "data-platform",
            },
            "operations": {
                "criticality": "high",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 240,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/s_customers_history",
            },
        },
    },
    "gold_full_refresh_kpi": {
        _TEMPLATE_META_KEY: {
            "name": "gold_full_refresh_kpi",
            "description": "Gold full refresh para tabela agregada/KPI.",
            "category": "gold",
            "files": ["ingestion", "annotations", "operations", "access"],
        },
        "ingestion": {
            "preset": "gold_full_refresh",
            "source": "sales_curated.s_orders",
            "target": _target("sales_mart", "g_daily_orders"),
            "layer": "gold",
            "schema_policy": "strict",
            "quality_rules": {
                "not_null": ["order_date"],
                "expressions": [
                    {
                        "name": "non_negative_revenue",
                        "expression": "gross_revenue >= 0",
                        "severity": "abort",
                        "message": "Receita agregada negativa",
                    }
                ],
            },
        },
        "annotations": {
            "target": _target("sales_mart", "g_daily_orders"),
            "table": {
                "description": "KPIs diários de pedidos para consumo executivo.",
                "tags": {"domain": "sales", "layer": "gold", "data_product": "orders"},
            },
            "columns": {
                "order_date": {"description": "Data de referência do KPI."},
                "gross_revenue": {"description": "Receita bruta diária."},
            },
        },
        "operations": {
            "target": _target("sales_mart", "g_daily_orders"),
            "ownership": {
                "business_owner": "sales-ops",
                "technical_owner": "analytics-engineering",
                "support_group": "analytics-engineering",
            },
            "operations": {
                "criticality": "critical",
                "expected_frequency": "daily",
                "freshness_sla_minutes": 360,
                "alert_on_failure": True,
                "alert_on_quality_fail": True,
                "runbook_url": "https://wiki.example.com/runbooks/g_daily_orders",
            },
        },
        "access": {
            "target": _target("sales_mart", "g_daily_orders"),
            "access_policy": {"mode": "validate_only", "on_drift": "warn", "revoke_unmanaged": False},
            "grants": [{"principal": "executive-dashboards", "privileges": ["SELECT"]}],
        },
    },
}


def list_contract_templates() -> list[str]:
    """Lista os nomes dos templates built-in."""

    return sorted(BUILTIN_CONTRACT_TEMPLATES)


def get_contract_template(name: str) -> ContractTemplate:
    """Retorna cópia defensiva de um template."""

    if name not in BUILTIN_CONTRACT_TEMPLATES:
        raise ValueError(f"Template não encontrado: {name}. Templates válidos: {list_contract_templates()}")
    return deepcopy(BUILTIN_CONTRACT_TEMPLATES[name])


def contract_template_details(name: str) -> dict[str, Any]:
    """Retorna metadados resumidos de um template."""

    template = get_contract_template(name)
    meta = dict(template.get(_TEMPLATE_META_KEY) or {})
    files = [key for key in ("ingestion", "annotations", "operations", "access") if key in template]
    return {
        "name": name,
        "description": meta.get("description", ""),
        "category": meta.get("category", "custom"),
        "files": files,
        "target": (template.get("ingestion") or {}).get("target"),
        "presets": (template.get("ingestion") or {}).get("preset"),
        "source": _template_source_kind(template),
        "mode": (template.get("ingestion") or {}).get("mode"),
        "recommendation_priority": meta.get("recommendation_priority", 100),
    }


def contract_template_files(name: str) -> dict[str, dict[str, Any]]:
    """Retorna arquivos lógicos de um template, sem metadados internos."""

    template = get_contract_template(name)
    return {
        key: deepcopy(template[key])
        for key in ("ingestion", "annotations", "operations", "access")
        if key in template
    }


def recommend_contract_templates(
    *,
    layer: str | None = None,
    source: str | None = None,
    mode: str | None = None,
    pattern: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Recomenda templates por cenário sem depender de prompt interativo."""

    criteria = {
        "layer": _norm(layer),
        "source": _norm(source),
        "mode": _norm(mode),
        "pattern": _norm(pattern),
    }
    has_criteria = any(criteria.values())
    recommendations = []
    for name in list_contract_templates():
        details = contract_template_details(name)
        haystack = _template_search_text(name)
        score = 0
        matched: list[str] = []
        if criteria["layer"] and criteria["layer"] == _norm(details.get("category")):
            score += 4
            matched.append("layer")
        if criteria["source"] and criteria["source"] in haystack:
            score += 3
            matched.append("source")
        if criteria["mode"] and criteria["mode"] in haystack:
            score += 3
            matched.append("mode")
        if criteria["pattern"] and criteria["pattern"] in haystack:
            score += 2
            matched.append("pattern")
        if has_criteria and score == 0:
            continue
        recommendations.append({**details, "score": score, "matched": matched})
    recommendations.sort(
        key=lambda item: (-int(item["score"]), int(item.get("recommendation_priority", 100)), str(item["name"]))
    )
    if limit is not None:
        return recommendations[: max(0, int(limit))]
    return recommendations


def _template_source_kind(template: ContractTemplate) -> str:
    ingestion = template.get("ingestion") or {}
    source = ingestion.get("source")
    if isinstance(source, str):
        return "table"
    if isinstance(source, dict):
        return str(source.get("connector") or source.get("type") or "connector")
    return "unknown"


def _template_search_text(name: str) -> str:
    payload = {
        "name": name,
        "details": contract_template_details(name),
        "template": get_contract_template(name),
    }
    return _norm(json.dumps(payload, ensure_ascii=False, default=str))


def _norm(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")
