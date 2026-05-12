"""JSON Schema para contratos YAML/JSON de ingestão."""
from __future__ import annotations

from typing import Any, Dict

from .config import (
    VALID_EXPLAIN_FORMATS,
    VALID_IDEMPOTENCY_POLICIES,
    VALID_LAYERS,
    VALID_MERGE_STRATEGIES,
    VALID_QUALITY_FAIL_ACTIONS,
    VALID_QUALITY_RULE_SEVERITIES,
    VALID_SCHEMA_POLICIES,
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
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://github.com/marquesantero/lakehouse-ingestion-framework/schema.json",
        "title": "Lakehouse Ingestion Contract",
        "type": "object",
        "additionalProperties": False,
        "required": ["source", "target_table"],
        "properties": {
            "source": {"type": "string"},
            "target_table": {"type": "string"},
            "catalog": {"type": "string"},
            "layer": {"enum": sorted(VALID_LAYERS)},
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
            "parent_run_id": {"type": ["string", "null"]},
            "run_group_id": {"type": ["string", "null"]},
            "master_job_id": {"type": ["string", "null"]},
            "master_run_id": {"type": ["string", "null"]},
        },
    }
