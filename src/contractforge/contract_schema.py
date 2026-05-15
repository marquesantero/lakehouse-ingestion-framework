"""JSON Schema para contratos YAML/JSON de ingestão."""
from __future__ import annotations

from typing import Any, Dict

from .config import (
    VALID_ACCESS_DRIFT_POLICIES,
    VALID_ACCESS_MODES,
    VALID_ACCESS_PRIVILEGES,
    VALID_CRITICALITY_LEVELS,
    VALID_EXPECTED_FREQUENCIES,
    VALID_EXPLAIN_FORMATS,
    VALID_GOVERNANCE_FAILURE_POLICIES,
    VALID_IDEMPOTENCY_POLICIES,
    VALID_LAYERS,
    VALID_MERGE_STRATEGIES,
    VALID_PII_TYPES,
    VALID_QUALITY_FAIL_ACTIONS,
    VALID_QUALITY_RULE_SEVERITIES,
    VALID_SCHEMA_POLICIES,
    VALID_SENSITIVITY_LEVELS,
    VALID_SOURCE_TRIGGERS,
    VALID_SOURCE_TYPES,
    VALID_WRITE_MODES,
)


def yaml_schema() -> Dict[str, Any]:
    """Retorna JSON Schema para autocomplete/validação de contratos."""
    string_array = {"type": "array", "items": {"type": "string"}}
    string_map = {
        "type": "object",
        "additionalProperties": {"type": "string"},
    }
    value_map = {
        "type": "object",
        "additionalProperties": {
            "oneOf": [
                {"type": "array"},
                {"type": "string"},
                {"type": "number"},
                {"type": "boolean"},
                {"type": "null"},
            ]
        },
    }
    pii_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "enabled": {"type": "boolean"},
            "type": {"enum": sorted(VALID_PII_TYPES)},
            "sensitivity": {"enum": sorted(VALID_SENSITIVITY_LEVELS)},
        },
    }
    deprecated_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "since": {"type": ["string", "null"]},
            "replacement": {"type": ["string", "null"]},
            "removal_date": {"type": ["string", "null"]},
        },
    }
    connector_source_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["type", "connector"],
        "properties": {
            "type": {"const": "connector"},
            "connector": {
                "type": "string",
                "pattern": "^[A-Za-z][A-Za-z0-9_-]*$",
                "description": "Nome do resolver registrado. Pode ser nativo ou customizado.",
            },
            "name": {"type": ["string", "null"]},
            "provider": {"type": ["string", "null"]},
            "format": {"type": ["string", "null"]},
            "path": {"type": ["string", "null"]},
            "account_url": {"type": ["string", "null"]},
            "container": {"type": ["string", "null"]},
            "table": {"type": ["string", "null"]},
            "query": {"type": ["string", "null"]},
            "options": {"type": "object"},
            "read": {"type": "object"},
            "request": {"type": "object"},
            "auth": {"type": "object"},
            "pagination": {"type": "object"},
            "response": {"type": "object"},
            "incremental": {"type": "object"},
            "limits": {"type": "object"},
        },
    }
    autoloader_source_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["type", "path", "schema_location", "checkpoint_location"],
        "properties": {
            "type": {"enum": sorted(VALID_SOURCE_TYPES - {"connector"})},
            "path": {"type": "string"},
            "format": {"type": "string"},
            "schema_location": {"type": "string"},
            "checkpoint_location": {"type": "string"},
            "trigger": {"enum": sorted(VALID_SOURCE_TRIGGERS)},
            "options": {"type": "object"},
            "schema_hints": {"type": ["string", "null"]},
            "include_existing_files": {"type": "boolean"},
            "max_files_per_trigger": {"type": "integer", "minimum": 1},
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://github.com/marquesantero/contractforge/schema.json",
        "title": "ContractForge Contract Schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["source"],
        "anyOf": [
            {"required": ["target_table"]},
            {
                "required": ["target"],
                "properties": {"target": {"required": ["table"]}},
            },
        ],
        "properties": {
            "_metadata": {"type": "object"},
            "preset": {"oneOf": [string_array, {"type": "string"}]},
            "presets": {"oneOf": [string_array, {"type": "string"}]},
            "applied_presets": string_array,
            "target": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "catalog": {"type": "string"},
                    "schema": {"type": "string"},
                    "table": {"type": "string"},
                },
            },
            "source": {
                "oneOf": [
                    {"type": "string"},
                    autoloader_source_schema,
                    connector_source_schema,
                ]
            },
            "target_table": {"type": "string"},
            "catalog": {"type": "string"},
            "layer": {"enum": sorted(VALID_LAYERS)},
            "target_schema": {"type": ["string", "null"]},
            "mode": {"enum": sorted(VALID_WRITE_MODES)},
            "source_system": {"type": "string"},
            "ctrl_schema": {"type": "string"},
            "notebook_name": {"type": "string"},
            "description": {"type": ["string", "null"]},
            "owner": {"type": ["string", "null"]},
            "domain": {"type": ["string", "null"]},
            "tags": string_array,
            "sla": {"type": ["string", "null"]},
            "runtime_parameters": {"type": "object"},
            "select_columns": {"oneOf": [string_array, {"type": "string"}]},
            "column_mapping": string_map,
            "shape": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "allow_cardinality_change_on_bronze": {"type": "boolean"},
                    "parse_json": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["column", "schema"],
                            "properties": {
                                "column": {"type": "string"},
                                "schema": {"type": "string"},
                                "alias": {"type": ["string", "null"]},
                                "drop_source": {"type": "boolean"},
                            },
                        },
                    },
                    "flatten": {
                        "oneOf": [
                            {"type": "boolean"},
                            {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "enabled": {"type": "boolean"},
                                    "separator": {"type": "string"},
                                    "include": {"oneOf": [string_array, {"type": "string"}]},
                                    "exclude": {"oneOf": [string_array, {"type": "string"}]},
                                    "max_depth": {"type": "integer", "minimum": 1},
                                },
                            },
                        ],
                    },
                    "arrays": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["path"],
                            "properties": {
                                "path": {"type": "string"},
                                "mode": {
                                    "enum": ["keep", "to_json", "size", "first", "explode", "explode_outer"]
                                },
                                "alias": {"type": ["string", "null"]},
                                "allow_cartesian": {"type": "boolean"},
                            },
                        },
                    },
                    "zip_arrays": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["alias", "columns"],
                            "properties": {
                                "alias": {"type": "string"},
                                "columns": {
                                    "type": "object",
                                    "minProperties": 2,
                                    "additionalProperties": {"type": "string"},
                                },
                            },
                        },
                    },
                    "columns": {
                        "type": "object",
                        "additionalProperties": {
                            "oneOf": [
                                {"type": "string"},
                                {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "alias": {"type": "string"},
                                        "cast": {"type": "string"},
                                        "expression": {"type": "string"},
                                    },
                                },
                            ]
                        },
                    },
                },
            },
            "filter_expression": {"type": ["string", "null"]},
            "watermark_columns": {"oneOf": [string_array, {"type": "string"}]},
            "merge_keys": {"oneOf": [string_array, {"type": "string"}]},
            "hash_keys": {"oneOf": [string_array, {"type": "string"}]},
            "hash_exclude_columns": {"oneOf": [string_array, {"type": "string"}]},
            "custom_keys": {"type": "object"},
            "dedup_order_expr": {"type": ["string", "null"]},
            "partition_column": {"type": ["string", "null"]},
            "partition_value": {"type": ["string", "null"]},
            "merge_strategy": {"enum": sorted(VALID_MERGE_STRATEGIES)},
            "merge_partition_column": {"type": ["string", "null"]},
            "replace_partitions_source_complete": {"type": "boolean"},
            "cluster_columns": {"oneOf": [string_array, {"type": "string"}]},
            "zorder_columns": {"oneOf": [string_array, {"type": "string"}]},
            "optimize_after_write": {"type": "boolean"},
            "delta_properties": string_map,
            "schema_policy": {"enum": sorted(VALID_SCHEMA_POLICIES)},
            "allow_type_widening": {"type": "boolean"},
            "quality_rules": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "required_columns": {"oneOf": [string_array, {"type": "string"}]},
                    "not_null": {"oneOf": [string_array, {"type": "string"}]},
                    "unique_key": {"oneOf": [string_array, {"type": "string"}]},
                    "accepted_values": value_map,
                    "min_rows": {"type": "integer", "minimum": 1},
                    "max_null_ratio": {
                        "type": "object",
                        "additionalProperties": {"type": "number", "minimum": 0, "maximum": 1},
                    },
                    "expressions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "expression"],
                            "properties": {
                                "name": {"type": "string"},
                                "expression": {"type": "string"},
                                "severity": {"enum": sorted(VALID_QUALITY_RULE_SEVERITIES)},
                                "message": {"type": ["string", "null"]},
                            },
                        },
                    },
                    "custom": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "required": ["type"],
                            "properties": {
                                "type": {"type": "string"},
                                "severity": {"enum": sorted(VALID_QUALITY_RULE_SEVERITIES)},
                                "message": {"type": ["string", "null"]},
                            },
                            "additionalProperties": True,
                        },
                    },
                },
            },
            "on_quality_fail": {"enum": sorted(VALID_QUALITY_FAIL_ACTIONS)},
            "scd2_change_columns": {"oneOf": [string_array, {"type": "string"}]},
            "scd2_effective_from_column": {"type": ["string", "null"]},
            "fix_encoding": {"type": "boolean"},
            "encoding": {"type": "string"},
            "encoding_columns": {"oneOf": [string_array, {"type": "string"}]},
            "dry_run": {"type": "boolean"},
            "explain_mode": {"type": "boolean"},
            "explain_format": {"enum": sorted(VALID_EXPLAIN_FORMATS)},
            "openlineage_enabled": {"type": "boolean"},
            "openlineage_namespace": {"type": ["string", "null"]},
            "openlineage_producer": {"type": "string"},
            "use_cache": {"type": "boolean"},
            "lock_enabled": {"type": "boolean"},
            "idempotency_key": {"type": ["string", "null"]},
            "idempotency_policy": {"enum": sorted(VALID_IDEMPOTENCY_POLICIES)},
            "retry_attempts": {"type": "integer", "minimum": 1},
            "retry_backoff_seconds": {"type": "integer", "minimum": 0},
            "annotations": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "policy": {"enum": sorted(VALID_GOVERNANCE_FAILURE_POLICIES)},
                    "target": {
                        "type": "object",
                        "properties": {
                            "catalog": {"type": "string"},
                            "schema": {"type": "string"},
                            "table": {"type": "string"},
                        },
                    },
                    "table": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "description": {"type": ["string", "null"]},
                            "aliases": {"oneOf": [string_array, {"type": "string"}]},
                            "tags": string_map,
                            "deprecated": deprecated_schema,
                        },
                    },
                    "columns": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "description": {"type": ["string", "null"]},
                                "aliases": {"oneOf": [string_array, {"type": "string"}]},
                                "tags": string_map,
                                "pii": pii_schema,
                                "deprecated": deprecated_schema,
                            },
                        },
                    },
                },
            },
            "operations": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "target": {
                        "type": "object",
                        "properties": {
                            "catalog": {"type": "string"},
                            "schema": {"type": "string"},
                            "table": {"type": "string"},
                        },
                    },
                    "ownership": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "business_owner": {"type": ["string", "null"]},
                            "technical_owner": {"type": ["string", "null"]},
                            "steward": {"type": ["string", "null"]},
                            "support_group": {"type": ["string", "null"]},
                            "escalation_group": {"type": ["string", "null"]},
                        },
                    },
                    "operations": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "criticality": {"enum": sorted(VALID_CRITICALITY_LEVELS)},
                            "expected_frequency": {"enum": sorted(VALID_EXPECTED_FREQUENCIES)},
                            "freshness_sla_minutes": {"type": "integer", "minimum": 1},
                            "alert_on_failure": {"type": "boolean"},
                            "alert_on_quality_fail": {"type": "boolean"},
                            "runbook_url": {"type": ["string", "null"]},
                            "owners": {"oneOf": [string_array, {"type": "string"}]},
                            "groups": {"oneOf": [string_array, {"type": "string"}]},
                            "tags": string_map,
                        },
                    },
                    "criticality": {"enum": sorted(VALID_CRITICALITY_LEVELS)},
                    "expected_frequency": {"enum": sorted(VALID_EXPECTED_FREQUENCIES)},
                    "freshness_sla_minutes": {"type": "integer", "minimum": 1},
                    "alert_on_failure": {"type": "boolean"},
                    "alert_on_quality_fail": {"type": "boolean"},
                    "runbook_url": {"type": ["string", "null"]},
                    "owners": {"oneOf": [string_array, {"type": "string"}]},
                    "groups": {"oneOf": [string_array, {"type": "string"}]},
                    "tags": string_map,
                },
            },
            "access": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "target": {
                        "type": "object",
                        "properties": {
                            "catalog": {"type": "string"},
                            "schema": {"type": "string"},
                            "table": {"type": "string"},
                        },
                    },
                    "access_policy": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "mode": {"enum": sorted(VALID_ACCESS_MODES)},
                            "on_drift": {"enum": sorted(VALID_ACCESS_DRIFT_POLICIES)},
                            "revoke_unmanaged": {"type": "boolean"},
                        },
                    },
                    "mode": {"enum": sorted(VALID_ACCESS_MODES)},
                    "on_drift": {"enum": sorted(VALID_ACCESS_DRIFT_POLICIES)},
                    "revoke_unmanaged": {"type": "boolean"},
                    "grants": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["principal", "privileges"],
                            "properties": {
                                "principal": {"type": "string"},
                                "privileges": {
                                    "oneOf": [
                                        {"type": "array", "items": {"enum": sorted(VALID_ACCESS_PRIVILEGES)}},
                                        {"enum": sorted(VALID_ACCESS_PRIVILEGES)},
                                    ]
                                },
                            },
                        },
                    },
                    "row_filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "function"],
                            "properties": {
                                "name": {"type": "string"},
                                "function": {"type": "string"},
                                "columns": {"oneOf": [string_array, {"type": "string"}]},
                                "applies_to": {
                                    "type": "object",
                                    "properties": {
                                        "principals": {"oneOf": [string_array, {"type": "string"}]},
                                    },
                                },
                            },
                        },
                    },
                    "column_masks": {
                        "oneOf": [
                            {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["column", "function"],
                                    "properties": {
                                        "column": {"type": "string"},
                                        "function": {"type": "string"},
                                        "using_columns": {"oneOf": [string_array, {"type": "string"}]},
                                        "applies_to": {
                                            "type": "object",
                                            "properties": {
                                                "principals": {"oneOf": [string_array, {"type": "string"}]},
                                            },
                                        },
                                    },
                                },
                            },
                            {
                                "type": "object",
                                "additionalProperties": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["function"],
                                    "properties": {
                                        "function": {"type": "string"},
                                        "using_columns": {"oneOf": [string_array, {"type": "string"}]},
                                        "applies_to": {
                                            "type": "object",
                                            "properties": {
                                                "principals": {"oneOf": [string_array, {"type": "string"}]},
                                            },
                                        },
                                    },
                                },
                            },
                        ],
                    },
                },
            },
            "parent_run_id": {"type": ["string", "null"]},
            "run_group_id": {"type": ["string", "null"]},
            "master_job_id": {"type": ["string", "null"]},
            "master_run_id": {"type": ["string", "null"]},
        },
    }
