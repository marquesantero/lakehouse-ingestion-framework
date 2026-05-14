"""Configuração global, tipos compartilhados e constantes do framework.

Este módulo é folha (não depende de outros do pacote) e é importado por todos
os demais. Define ``Literal``s para narrowing estático e a singleton ``CONFIG``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union

from pyspark.sql import DataFrame

FRAMEWORK_VERSION = "1.9.0"
CTRL_SCHEMA_VERSION = 11

#: Camadas reconhecidas (Medallion Architecture).
Layer = Literal["bronze", "silver", "gold"]

#: Modos oficiais de escrita; ver ``writers.py`` para a semântica de cada um.
WriteMode = Literal[
    "scd0_append",
    "scd0_overwrite",
    "scd1_upsert",
    "scd1_hash_diff",
    "scd2_historical",
    "snapshot_soft_delete",
]

#: Estratégia do MERGE em ``scd1_upsert``.
MergeStrategy = Literal["delta", "delta_by_partition", "replace_partitions"]

#: Política de evolução de schema do destino.
SchemaPolicy = Literal["permissive", "additive_only", "strict"]

#: Ação global legada quando regras built-in de qualidade falham.
QualityFailAction = Literal["fail", "warn", "quarantine"]

#: Severidade por regra de qualidade.
QualityRuleSeverity = Literal["warn", "quarantine", "abort"]

#: Politica de falha ao aplicar anotacoes de catalogo.
GovernanceFailurePolicy = Literal["fail", "warn", "ignore"]

#: Modo de aplicacao de contratos de acesso.
AccessMode = Literal["apply", "validate_only", "ignore"]

#: Politica quando o contrato de acesso encontra drift ou falha de aplicacao.
AccessDriftPolicy = Literal["fail", "warn", "reconcile"]

#: Política de idempotência para uma ``idempotency_key`` lógica.
IdempotencyPolicy = Literal["always_run", "skip_if_success", "fail_if_success", "rerun_if_failed"]

#: Fonte aceita pelo plano: nome de tabela, DataFrame em memória ou source declarativo.
Source = Union[str, DataFrame, "SourceSpec", "ConnectorSpec"]  # noqa: F821

#: Conjunto usado em validação runtime (Literal só faz tipagem estática).
VALID_WRITE_MODES = {
    "scd0_append",
    "scd0_overwrite",
    "scd1_upsert",
    "scd1_hash_diff",
    "scd2_historical",
    "snapshot_soft_delete",
}

#: Camadas válidas para validação runtime.
VALID_LAYERS = {"bronze", "silver", "gold"}

#: Estratégias de merge válidas para validação runtime.
VALID_MERGE_STRATEGIES = {"delta", "delta_by_partition", "replace_partitions"}

#: Políticas de schema válidas para validação runtime.
VALID_SCHEMA_POLICIES = {"permissive", "additive_only", "strict"}

#: Ações válidas em falha de qualidade para validação runtime.
VALID_QUALITY_FAIL_ACTIONS = {"fail", "warn", "quarantine"}

#: Severidades válidas em regras de qualidade declarativas.
VALID_QUALITY_RULE_SEVERITIES = {"warn", "quarantine", "abort"}

VALID_GOVERNANCE_FAILURE_POLICIES = {"fail", "warn", "ignore"}

VALID_ACCESS_MODES = {"apply", "validate_only", "ignore"}

VALID_ACCESS_DRIFT_POLICIES = {"fail", "warn", "reconcile"}

VALID_CRITICALITY_LEVELS = {"low", "medium", "high", "critical"}

VALID_EXPECTED_FREQUENCIES = {"hourly", "daily", "weekly", "monthly", "ad_hoc"}

VALID_SENSITIVITY_LEVELS = {"public", "internal", "restricted", "confidential"}

VALID_PII_TYPES = {
    "address",
    "bank_account",
    "birth_date",
    "credit_card",
    "device_id",
    "document",
    "email",
    "financial",
    "health",
    "ip_address",
    "name",
    "national_id",
    "other",
    "phone",
    "ssn",
    "tax_id",
    "unknown",
}

VALID_ACCESS_PRIVILEGES = {
    "ALL PRIVILEGES",
    "APPLY TAG",
    "CREATE",
    "CREATE FUNCTION",
    "CREATE MODEL",
    "CREATE TABLE",
    "CREATE VOLUME",
    "EXECUTE",
    "MANAGE",
    "MODIFY",
    "READ FILES",
    "READ VOLUME",
    "REFRESH",
    "SELECT",
    "USAGE",
    "WRITE FILES",
    "WRITE VOLUME",
}

#: Políticas válidas de idempotência para validação runtime.
VALID_IDEMPOTENCY_POLICIES = {"always_run", "skip_if_success", "fail_if_success", "rerun_if_failed"}

#: Formatos de explain aceitos por ``DataFrame.explain``.
VALID_EXPLAIN_FORMATS = {"simple", "extended", "codegen", "cost", "formatted"}

#: Tipos de source declarativo aceitos.
VALID_SOURCE_TYPES = {"autoloader", "connector"}

#: Conectores nativos de source. Conectores customizados podem ser registrados
#: via ``register_source_resolver`` sem alterar esta lista.
VALID_SOURCE_CONNECTORS = {
    "autoloader",
    "blob",
    "csv",
    "delta_table",
    "jdbc",
    "json",
    "object_storage",
    "parquet",
    "rest_api",
    "sql",
    "table",
    "text",
    "view",
}

VALID_OBJECT_STORAGE_PROVIDERS = {"adls", "azure_blob", "gcs", "s3"}

VALID_FILE_CONNECTOR_FORMATS = {"csv", "delta", "json", "orc", "parquet", "text"}

#: Triggers aceitos para sources declarativos.
VALID_SOURCE_TRIGGERS = {"available_now"}

#: Colunas gerenciadas pelo framework. Excluídas do hash determinístico em
#: ``schema.hash_columns`` para que mudanças em controle não invalidem
#: ``row_hash``.
CONTROL_COLUMNS = {
    "ingestion_date",
    "ingestion_ts_utc",
    "source_system",
    "__run_id",
    "row_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "is_active",
    "deleted_at",
    "changed_columns",
}


@dataclass(frozen=True)
class FrameworkConfig:
    """Configuração global do framework.

    Imutável. A instância padrão é ``CONFIG``. Para sobrescrever defaults em
    todo o processo, faça monkey-patch antes da primeira chamada:

    >>> import lakehouse_ingestion.config as cfg
    >>> cfg.CONFIG = cfg.FrameworkConfig(ctrl_schema="my_ops")

    Em prática, prefira passar ``ctrl_schema``/etc. no ``IngestionPlan``.

    Attributes:
        default_catalog: Catálogo Unity quando não especificado no plan.
        default_source_system: ``source_system`` quando não informado.
        default_partition_col: Coluna de partição padrão (``ingestion_date``).
        ctrl_schema: Schema onde as ctrl tables vivem.
        ctrl_table_*: Nomes das ctrl tables.
        max_error_len: Tamanho máximo de ``error_message`` em ctrl tables.
        default_lock_ttl_minutes: TTL do lock best-effort em ``acquire_lock``.
        default_retry_attempts: Tentativas em ``with_retry`` para conflitos Delta.
        default_retry_backoff_seconds: Backoff linear entre tentativas.
        max_inline_accepted_values: Limite de itens em ``accepted_values``.
        max_partition_predicate_values: Limite de valores em predicados ``IN``.
    """

    default_catalog: str = "main"
    default_source_system: str = "default"
    default_partition_col: str = "ingestion_date"
    ctrl_schema: str = "ops"
    ctrl_table_runs: str = "ctrl_ingestion_runs"
    ctrl_table_state: str = "ctrl_ingestion_state"
    ctrl_table_quality: str = "ctrl_ingestion_quality"
    ctrl_table_quarantine: str = "ctrl_ingestion_quarantine"
    ctrl_table_locks: str = "ctrl_ingestion_locks"
    ctrl_table_explain: str = "ctrl_ingestion_explain"
    ctrl_table_lineage: str = "ctrl_ingestion_lineage"
    ctrl_table_metadata: str = "ctrl_ingestion_metadata"
    ctrl_table_errors: str = "ctrl_ingestion_errors"
    ctrl_table_schema_changes: str = "ctrl_ingestion_schema_changes"
    ctrl_table_streams: str = "ctrl_ingestion_streams"
    ctrl_table_annotations: str = "ctrl_ingestion_annotations"
    ctrl_table_operations: str = "ctrl_ingestion_operations"
    ctrl_table_access: str = "ctrl_ingestion_access"
    max_error_len: int = 8000
    default_lock_ttl_minutes: int = 120
    default_retry_attempts: int = 3
    default_retry_backoff_seconds: int = 5
    max_inline_accepted_values: int = 1000
    max_partition_predicate_values: int = 1000


#: Singleton de configuração. Importada por outros módulos.
CONFIG = FrameworkConfig()
