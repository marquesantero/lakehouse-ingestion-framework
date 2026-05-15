"""Presets declarativos para padrões comuns de ingestão.

Presets são defaults opinativos: reduzem repetição, mas o contrato do usuário
sempre vence. Use ``register_preset`` para acoplar presets internos da empresa
sem alterar o core da lib.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from ._sql import as_list

Preset = Dict[str, Any]


def _meta(
    *,
    name: str,
    description: str,
    category: str,
    kind: str,
    required_fields: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "category": category,
        "kind": kind,
        "required_fields": required_fields or [],
    }


_RESERVED_PRESET_KEY = "_preset"


BUILTIN_PRESETS: dict[str, Preset] = {
    "bronze_autoloader_append": {
        "_preset": _meta(
            name="bronze_autoloader_append",
            description="Bronze append com Auto Loader em available_now.",
            category="bronze",
            kind="ingestion",
            required_fields=[
                "source.path",
                "source.schema_location",
                "source.checkpoint_location",
                "target_table",
            ],
        ),
        "source": {"type": "autoloader", "trigger": "available_now", "format": "parquet"},
        "layer": "bronze",
        "mode": "scd0_append",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
        "idempotency_policy": "skip_if_success",
    },
    "bronze_file_append": {
        "_preset": _meta(
            name="bronze_file_append",
            description="Bronze append para arquivo/batch já resolvido pelo usuário.",
            category="bronze",
            kind="ingestion",
        ),
        "layer": "bronze",
        "mode": "scd0_append",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "bronze_table_append": {
        "_preset": _meta(
            name="bronze_table_append",
            description="Bronze append para replicação simples table-to-table.",
            category="bronze",
            kind="ingestion",
        ),
        "layer": "bronze",
        "mode": "scd0_append",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "bronze_full_overwrite": {
        "_preset": _meta(
            name="bronze_full_overwrite",
            description="Bronze com snapshot completo sobrescrevendo a tabela.",
            category="bronze",
            kind="ingestion",
        ),
        "layer": "bronze",
        "mode": "scd0_overwrite",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "bronze_partition_overwrite": {
        "_preset": _meta(
            name="bronze_partition_overwrite",
            description="Bronze com sobrescrita controlada de uma partição.",
            category="bronze",
            kind="ingestion",
            required_fields=["partition_column", "partition_value"],
        ),
        "layer": "bronze",
        "mode": "scd0_overwrite",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_scd1_upsert": {
        "_preset": _meta(
            name="silver_scd1_upsert",
            description="Silver estado atual com MERGE SCD1.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "silver",
        "mode": "scd1_upsert",
        "merge_strategy": "delta",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_scd1_partition_upsert": {
        "_preset": _meta(
            name="silver_scd1_partition_upsert",
            description="Silver SCD1 com MERGE podado por partição.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys", "merge_partition_column"],
        ),
        "layer": "silver",
        "mode": "scd1_upsert",
        "merge_strategy": "delta_by_partition",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_replace_partitions": {
        "_preset": _meta(
            name="silver_replace_partitions",
            description="Silver com substituição de partições completas.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_partition_column"],
        ),
        "layer": "silver",
        "mode": "scd1_upsert",
        "merge_strategy": "replace_partitions",
        "replace_partitions_source_complete": True,
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_hash_diff_append": {
        "_preset": _meta(
            name="silver_hash_diff_append",
            description="Silver append somente para novas chaves ou mudança de hash.",
            category="silver",
            kind="ingestion",
            required_fields=["hash_keys"],
        ),
        "layer": "silver",
        "mode": "scd1_hash_diff",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
        "hash_exclude_columns": ["ingestion_date", "ingestion_ts_utc", "source_system", "__run_id"],
    },
    "silver_snapshot_soft_delete": {
        "_preset": _meta(
            name="silver_snapshot_soft_delete",
            description="Silver snapshot completo com soft delete de ausentes.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "silver",
        "mode": "snapshot_soft_delete",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_scd2_historical": {
        "_preset": _meta(
            name="silver_scd2_historical",
            description="Silver histórico SCD2 com versões correntes e expiradas.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "silver",
        "mode": "scd2_historical",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_incremental_watermark_upsert": {
        "_preset": _meta(
            name="silver_incremental_watermark_upsert",
            description="Silver SCD1 incremental controlado por watermark.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys", "watermark_columns"],
        ),
        "layer": "silver",
        "mode": "scd1_upsert",
        "merge_strategy": "delta",
        "schema_policy": "additive_only",
        "on_quality_fail": "fail",
    },
    "silver_quarantine_ingestion": {
        "_preset": _meta(
            name="silver_quarantine_ingestion",
            description="Silver SCD1 com quarentena para regras linha-a-linha.",
            category="silver",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "silver",
        "mode": "scd1_upsert",
        "merge_strategy": "delta",
        "schema_policy": "additive_only",
        "on_quality_fail": "quarantine",
    },
    "gold_full_refresh": {
        "_preset": _meta(
            name="gold_full_refresh",
            description="Gold recalculada por refresh total.",
            category="gold",
            kind="ingestion",
        ),
        "layer": "gold",
        "mode": "scd0_overwrite",
        "schema_policy": "strict",
        "on_quality_fail": "fail",
    },
    "gold_partition_refresh": {
        "_preset": _meta(
            name="gold_partition_refresh",
            description="Gold recalculada por partição.",
            category="gold",
            kind="ingestion",
            required_fields=["partition_column", "partition_value"],
        ),
        "layer": "gold",
        "mode": "scd0_overwrite",
        "schema_policy": "strict",
        "on_quality_fail": "fail",
    },
    "gold_replace_partitions": {
        "_preset": _meta(
            name="gold_replace_partitions",
            description="Gold com substituição declarativa de partições completas.",
            category="gold",
            kind="ingestion",
            required_fields=["merge_partition_column"],
        ),
        "layer": "gold",
        "mode": "scd1_upsert",
        "merge_strategy": "replace_partitions",
        "replace_partitions_source_complete": True,
        "schema_policy": "strict",
        "on_quality_fail": "fail",
    },
    "gold_snapshot_serving": {
        "_preset": _meta(
            name="gold_snapshot_serving",
            description="Gold serving com snapshot e soft delete.",
            category="gold",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "gold",
        "mode": "snapshot_soft_delete",
        "schema_policy": "strict",
        "on_quality_fail": "fail",
    },
    "gold_scd1_serving": {
        "_preset": _meta(
            name="gold_scd1_serving",
            description="Gold serving corrente com SCD1.",
            category="gold",
            kind="ingestion",
            required_fields=["merge_keys"],
        ),
        "layer": "gold",
        "mode": "scd1_upsert",
        "merge_strategy": "delta",
        "schema_policy": "strict",
        "on_quality_fail": "fail",
    },
    "quality_strict": {
        "_preset": _meta(
            name="quality_strict",
            description="Política de qualidade abortiva.",
            category="quality",
            kind="modifier",
        ),
        "on_quality_fail": "fail",
    },
    "quality_quarantine": {
        "_preset": _meta(
            name="quality_quarantine",
            description="Política de qualidade com quarentena quando possível.",
            category="quality",
            kind="modifier",
        ),
        "on_quality_fail": "quarantine",
    },
    "delta_cdf_enabled": {
        "_preset": _meta(
            name="delta_cdf_enabled",
            description="Habilita Change Data Feed na tabela Delta criada.",
            category="delta",
            kind="modifier",
        ),
        "delta_properties": {"delta.enableChangeDataFeed": "true"},
    },
    "delta_optimized_writes": {
        "_preset": _meta(
            name="delta_optimized_writes",
            description="Habilita propriedades Delta de escrita/compactação otimizadas.",
            category="delta",
            kind="modifier",
        ),
        "delta_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        },
    },
    "runtime_databricks_serverless": {
        "_preset": _meta(
            name="runtime_databricks_serverless",
            description="Defaults seguros para Databricks Serverless/Spark Connect.",
            category="runtime",
            kind="runtime",
        ),
        "use_cache": False,
        "optimize_after_write": False,
    },
    "runtime_spark_delta_local": {
        "_preset": _meta(
            name="runtime_spark_delta_local",
            description="Defaults seguros para testes locais com PySpark + Delta.",
            category="runtime",
            kind="runtime",
        ),
        "use_cache": False,
        "optimize_after_write": False,
        "lock_enabled": False,
    },
    "governance_uc_basic": {
        "_preset": _meta(
            name="governance_uc_basic",
            description="Governança UC básica com annotations tolerantes e access validate_only.",
            category="governance",
            kind="modifier",
        ),
        "annotations": {"policy": "warn"},
        "access": {"access_policy": {"mode": "validate_only", "on_drift": "warn"}},
    },
}

PRESETS: dict[str, Preset] = deepcopy(BUILTIN_PRESETS)


def list_presets() -> list[str]:
    """Lista nomes de presets disponíveis."""
    return sorted(PRESETS)


def get_preset(name: str) -> Preset:
    """Retorna uma cópia defensiva do preset."""
    if name not in PRESETS:
        raise ValueError(f"Preset não encontrado: {name}. Presets válidos: {list_presets()}")
    return deepcopy(PRESETS[name])


def register_preset(name: str, preset: Preset, *, override: bool = False) -> None:
    """Registra preset customizado em runtime.

    Args:
        name: Nome usado no contrato.
        preset: Dicionário com defaults do contrato.
        override: Permite sobrescrever preset existente.
    """
    normalized_name = str(name or "").strip()
    if not normalized_name:
        raise ValueError("name do preset não pode ser vazio")
    if not isinstance(preset, dict):
        raise ValueError("preset deve ser um dict")
    if normalized_name in PRESETS and not override:
        raise ValueError(f"Preset já registrado: {normalized_name}")
    payload = deepcopy(preset)
    meta = dict(payload.get(_RESERVED_PRESET_KEY) or {})
    meta.setdefault("name", normalized_name)
    meta.setdefault("description", "")
    meta.setdefault("category", "custom")
    meta.setdefault("kind", "modifier")
    meta.setdefault("required_fields", [])
    payload[_RESERVED_PRESET_KEY] = meta
    PRESETS[normalized_name] = payload


def apply_preset(contract: dict[str, Any]) -> dict[str, Any]:
    """Expande ``preset``/``presets`` em um contrato final.

    O contrato explícito sempre vence os defaults do preset.
    """
    if not isinstance(contract, dict):
        raise ValueError("contract deve ser dict para aplicar presets")
    names = _preset_names(contract)
    if not names:
        expanded = _copy_user_mapping(contract)
        expanded.setdefault("applied_presets", [])
        return expanded

    preset_payload: dict[str, Any] = {}
    metas = []
    for name in names:
        preset = get_preset(name)
        meta = dict(preset.pop(_RESERVED_PRESET_KEY, {}))
        metas.append(meta)
        preset_payload = _deep_merge(preset_payload, preset)

    _validate_preset_combination(metas)
    explicit = _copy_user_mapping(contract)
    explicit.pop("preset", None)
    explicit.pop("presets", None)
    explicit.pop("applied_presets", None)
    expanded = _deep_merge(preset_payload, explicit)
    expanded["applied_presets"] = names
    _validate_required_fields(expanded, metas)
    return expanded


def preset_details(name: str) -> dict[str, Any]:
    """Descrição resumida para CLI/docs."""
    preset = get_preset(name)
    meta = dict(preset.pop(_RESERVED_PRESET_KEY, {}))
    return {
        "name": name,
        "description": meta.get("description", ""),
        "category": meta.get("category", "custom"),
        "kind": meta.get("kind", "modifier"),
        "required_fields": list(meta.get("required_fields") or []),
        "sets": sorted(_flatten_keys(preset)),
    }


def _preset_names(contract: dict[str, Any]) -> list[str]:
    raw = contract.get("preset", contract.get("presets"))
    names = as_list(raw)
    if any(not name for name in names):
        raise ValueError("preset não pode conter valores vazios")
    return names


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
            and key != _RESERVED_PRESET_KEY
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = _copy_user_value(value)
    return result


def _copy_user_mapping(value: dict[str, Any]) -> dict[str, Any]:
    return {key: _copy_user_value(val) for key, val in value.items()}


def _copy_user_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _copy_user_mapping(value)
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return tuple(value)
    if isinstance(value, set):
        return set(value)
    return value


def _validate_preset_combination(metas: list[dict[str, Any]]) -> None:
    kinds: dict[str, list[str]] = {}
    for meta in metas:
        kind = str(meta.get("kind") or "modifier")
        kinds.setdefault(kind, []).append(str(meta.get("name") or "unknown"))
    for exclusive_kind in ("ingestion", "runtime"):
        if len(kinds.get(exclusive_kind, [])) > 1:
            raise ValueError(
                f"Presets do tipo {exclusive_kind} são exclusivos; recebidos: {kinds[exclusive_kind]}"
            )


def _validate_required_fields(contract: dict[str, Any], metas: list[dict[str, Any]]) -> None:
    missing = []
    for meta in metas:
        preset_name = str(meta.get("name") or "unknown")
        for field_path in meta.get("required_fields") or []:
            if not _has_value(contract, str(field_path)):
                missing.append(f"{preset_name}:{field_path}")
    if missing:
        raise ValueError(f"Campos obrigatórios ausentes para presets: {missing}")


def _has_value(contract: dict[str, Any], field_path: str) -> bool:
    current: Any = contract
    for part in field_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    if current is None:
        return False
    if isinstance(current, str):
        return bool(current.strip())
    if isinstance(current, (list, tuple, set, dict)):
        return bool(current)
    return True


def _flatten_keys(payload: dict[str, Any], prefix: str = "") -> List[str]:
    keys: list[str] = []
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            keys.extend(_flatten_keys(value, path))
        else:
            keys.append(path)
    return keys
