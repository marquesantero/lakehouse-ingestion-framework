"""Templates de contratos para cenários comuns de uso."""
from __future__ import annotations

from copy import deepcopy
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
    }


def contract_template_files(name: str) -> dict[str, dict[str, Any]]:
    """Retorna arquivos lógicos de um template, sem metadados internos."""

    template = get_contract_template(name)
    return {
        key: deepcopy(template[key])
        for key in ("ingestion", "annotations", "operations", "access")
        if key in template
    }
