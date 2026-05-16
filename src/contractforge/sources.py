"""Resolvers e conectores para fontes declarativas."""
from __future__ import annotations

import base64
import csv
import io
import importlib.util
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Tuple

from pyspark.sql import DataFrame, SparkSession

from ._spark import spark
from .config import VALID_FILE_CONNECTOR_FORMATS, VALID_HTTP_FILE_FORMATS, VALID_OBJECT_STORAGE_PROVIDERS
from .plan import ConnectorSpec, IngestionPlan, SourceSpec


_SECRET_MARKER = "***REDACTED***"
_SENSITIVE_KEY_PARTS = ("authorization", "password", "secret", "token", "api_key", "apikey", "key")
_CONNECTOR_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_SECRET_PLACEHOLDER_RE = re.compile(r"\{\{\s*secret:[^}]+\}\}", re.IGNORECASE)
_AUTH_HEADER_RE = re.compile(r"\b(Bearer|Basic)\s+[^,\s'\"}]+", re.IGNORECASE)
_URL_USERINFO_RE = re.compile(r"([a-z][a-z0-9+.-]*://)([^:/@\s]+):([^@\s]+)@", re.IGNORECASE)
_SIMPLE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SENSITIVE_PARAM_RE = re.compile(
    r"(?i)([?&;](?:password|passwd|pwd|token|access_token|refresh_token|secret|client_secret|api_key|apikey)=)"
    r"([^&;\s]+)"
)
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|token|access_token|refresh_token|secret|client_secret|api_key|apikey|authorization)"
    r"(\s*[:=]\s*)([^\s,;})\]]+)"
)
_JSON_LINES_FORMATS = {"jsonl", "ndjson"}


@dataclass(frozen=True)
class ConnectorCapabilities:
    """Capacidades declaradas por conector para validação e observabilidade."""

    batch: bool = True
    streaming: bool = False
    pushdown_filter: bool = False
    partitioned_read: bool = False
    incremental_read: bool = False
    schema_inference: bool = True
    requires_secrets: bool = False
    source_complete: bool = False


@dataclass(frozen=True)
class SourceResolution:
    """Resultado da resolução de source."""

    df: DataFrame
    label: str
    connector: str
    metadata: Dict[str, Any]
    capabilities: ConnectorCapabilities


class SourceResolver(Protocol):
    """Contrato de um resolver de source declarativo."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        """Declara capacidades para o source."""
        ...

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        """Resolve source como batch DataFrame."""
        ...

    def resolve_stream(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> Tuple[DataFrame, str]:
        """Resolve source como streaming DataFrame e devolve ``(df, label)``."""
        ...


SOURCE_RESOLVER_REGISTRY: Dict[str, SourceResolver] = {}

BUILTIN_CONNECTOR_METADATA: Dict[str, Dict[str, Any]] = {
    "autoloader": {
        "family": "streaming",
        "description": "Databricks Auto Loader em modo available_now.",
        "required": ["path", "format", "read.schema_location", "read.checkpoint_location"],
        "incremental": True,
    },
    "table": {
        "family": "catalog",
        "description": "Tabela do catálogo Spark/Unity Catalog.",
        "required": ["table"],
        "incremental": False,
    },
    "delta_table": {
        "family": "catalog",
        "description": "Tabela Delta registrada no catálogo.",
        "required": ["table"],
        "incremental": False,
    },
    "view": {
        "family": "catalog",
        "description": "View registrada no catálogo.",
        "required": ["table"],
        "incremental": False,
    },
    "sql": {
        "family": "catalog",
        "description": "Query SQL declarativa.",
        "required": ["query"],
        "incremental": False,
    },
    "parquet": {"family": "files", "description": "Arquivos Parquet batch.", "required": ["path"], "incremental": False},
    "delta": {"family": "files", "description": "Arquivos Delta por path.", "required": ["path"], "incremental": False},
    "json": {"family": "files", "description": "Arquivos JSON batch.", "required": ["path"], "incremental": False},
    "csv": {"family": "files", "description": "Arquivos CSV batch.", "required": ["path"], "incremental": False},
    "orc": {"family": "files", "description": "Arquivos ORC batch.", "required": ["path"], "incremental": False},
    "text": {"family": "files", "description": "Arquivos texto batch.", "required": ["path"], "incremental": False},
    "http_file": {
        "family": "http_files",
        "description": "Arquivo HTTP(S) baixado pelo driver Python e convertido para DataFrame Spark.",
        "required": ["path ou request.url", "format"],
        "incremental": False,
        "runtime": "Biblioteca padrão Python urllib; não depende de Spark filesystem para https://.",
    },
    "http_csv": {
        "family": "http_files",
        "description": "Alias de http_file com format=csv.",
        "required": ["path ou request.url"],
        "incremental": False,
        "runtime": "Biblioteca padrão Python urllib; não depende de Spark filesystem para https://.",
    },
    "http_json": {
        "family": "http_files",
        "description": "Alias de http_file com format=json.",
        "required": ["path ou request.url"],
        "incremental": False,
        "runtime": "Biblioteca padrão Python urllib; não depende de Spark filesystem para https://.",
    },
    "http_text": {
        "family": "http_files",
        "description": "Alias de http_file com format=text.",
        "required": ["path ou request.url"],
        "incremental": False,
        "runtime": "Biblioteca padrão Python urllib; não depende de Spark filesystem para https://.",
    },
    "object_storage": {
        "family": "object_storage",
        "description": "Arquivos em ADLS/Azure Blob/S3/GCS via Spark reader.",
        "required": ["provider", "format", "path"],
        "incremental": False,
    },
    "blob": {
        "family": "object_storage",
        "description": "Alias para object storage/blob storage.",
        "required": ["provider", "format", "path"],
        "incremental": False,
    },
    "s3": {
        "family": "object_storage",
        "description": "Arquivos em Amazon S3 via Spark reader.",
        "required": ["format", "path"],
        "incremental": False,
    },
    "adls": {
        "family": "object_storage",
        "description": "Arquivos em Azure Data Lake Storage via Spark reader.",
        "required": ["format", "path"],
        "incremental": False,
    },
    "azure_blob": {
        "family": "object_storage",
        "description": "Arquivos em Azure Blob Storage via Spark reader.",
        "required": ["format", "path"],
        "incremental": False,
    },
    "gcs": {
        "family": "object_storage",
        "description": "Arquivos em Google Cloud Storage via Spark reader.",
        "required": ["format", "path"],
        "incremental": False,
    },
    "jdbc": {
        "family": "external",
        "description": "Fonte JDBC via Spark JDBC reader.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "postgres": {
        "family": "external",
        "description": "Alias JDBC para PostgreSQL.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "postgresql": {
        "family": "external",
        "description": "Alias JDBC para PostgreSQL.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "sqlserver": {
        "family": "external",
        "description": "Alias JDBC para Microsoft SQL Server.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "mysql": {
        "family": "external",
        "description": "Alias JDBC para MySQL/MariaDB.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "oracle": {
        "family": "external",
        "description": "Alias JDBC para Oracle Database.",
        "required": ["options.url", "options.dbtable ou options.query"],
        "incremental": True,
    },
    "snowflake": {
        "family": "external",
        "description": "Fonte Snowflake via Spark Snowflake connector.",
        "required": ["connection options", "options.dbtable ou options.query"],
        "incremental": False,
    },
    "bigquery": {
        "family": "external",
        "description": "Fonte BigQuery via Spark BigQuery connector.",
        "required": ["table, options.table ou options.query"],
        "incremental": False,
    },
    "rest_api": {
        "family": "external",
        "description": "API REST JSON em batch com auth, paginação, retry e modo raw para shape.parse_json.",
        "required": ["request.url"],
        "incremental": True,
    },
}

CONNECTOR_RUNTIME_REQUIREMENTS: Dict[str, Dict[str, Any]] = {
    "autoloader": {
        "status": "runtime_required",
        "runtime": "Databricks Runtime com Auto Loader",
        "python_packages": [],
        "notes": ["Requer cloudFiles no runtime; não é validável sem executar em Databricks."],
    },
    "delta": {
        "status": "runtime_required",
        "runtime": "Delta Lake disponível no Spark",
        "python_packages": [],
        "notes": ["Em Spark local use o extra contractforge[spark] ou configure delta-spark manualmente."],
    },
    "object_storage": {
        "status": "runtime_required",
        "runtime": "Credenciais e conector cloud configurados no Spark",
        "python_packages": [],
        "notes": ["Acesso deve vir de UC external locations, volumes, instance profile ou service principal."],
    },
    "blob": {
        "status": "runtime_required",
        "runtime": "Credenciais e conector cloud configurados no Spark",
        "python_packages": [],
        "notes": ["Alias genérico de object storage; declare provider para auditoria."],
    },
    "s3": {
        "status": "runtime_required",
        "runtime": "Acesso S3 configurado no Spark",
        "python_packages": [],
        "notes": ["Requer credenciais/IAM e suporte Hadoop S3A no runtime."],
    },
    "adls": {
        "status": "runtime_required",
        "runtime": "Acesso ADLS configurado no Spark",
        "python_packages": [],
        "notes": ["Em Databricks prefira external locations ou volumes Unity Catalog."],
    },
    "azure_blob": {
        "status": "runtime_required",
        "runtime": "Acesso Azure Blob configurado no Spark",
        "python_packages": [],
        "notes": ["Em Databricks prefira external locations ou volumes Unity Catalog."],
    },
    "gcs": {
        "status": "runtime_required",
        "runtime": "Acesso GCS configurado no Spark",
        "python_packages": [],
        "notes": ["Requer connector/credenciais GCS disponíveis no runtime."],
    },
    "jdbc": {
        "status": "runtime_required",
        "runtime": "Driver JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Valide o driver no cluster/serverless antes de executar contratos produtivos."],
    },
    "postgres": {
        "status": "runtime_required",
        "runtime": "Driver PostgreSQL JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Alias JDBC; a lib não instala o driver."],
    },
    "postgresql": {
        "status": "runtime_required",
        "runtime": "Driver PostgreSQL JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Alias JDBC; a lib não instala o driver."],
    },
    "sqlserver": {
        "status": "runtime_required",
        "runtime": "Driver Microsoft SQL Server JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Alias JDBC; a lib não instala o driver."],
    },
    "mysql": {
        "status": "runtime_required",
        "runtime": "Driver MySQL/MariaDB JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Alias JDBC; a lib não instala o driver."],
    },
    "oracle": {
        "status": "runtime_required",
        "runtime": "Driver Oracle JDBC no classpath do Spark",
        "python_packages": [],
        "notes": ["Driver Oracle costuma exigir gestão/licença controlada pelo runtime."],
    },
    "snowflake": {
        "status": "runtime_required",
        "runtime": "Spark Snowflake connector instalado no runtime",
        "python_packages": [],
        "notes": ["Delegado a spark.read.format('snowflake'); valide JAR/package no cluster/serverless."],
    },
    "bigquery": {
        "status": "runtime_required",
        "runtime": "Spark BigQuery connector instalado no runtime",
        "python_packages": [],
        "notes": ["Delegado a spark.read.format('bigquery'); valide JAR/package no cluster/serverless."],
    },
    "rest_api": {
        "status": "ok",
        "runtime": "Python urllib + SparkSession ativa para materializar DataFrame",
        "python_packages": [],
        "notes": ["Não requer biblioteca HTTP externa; adequado para volumes controlados."],
    },
    "http_file": {
        "status": "ok",
        "runtime": "Python urllib + SparkSession ativa para materializar DataFrame",
        "python_packages": [],
        "notes": ["Não usa spark.read em https://; adequado para CSV/JSON/text de volume controlado."],
    },
    "http_csv": {
        "status": "ok",
        "runtime": "Python urllib + SparkSession ativa para materializar DataFrame",
        "python_packages": [],
        "notes": ["Alias de http_file com format=csv."],
    },
    "http_json": {
        "status": "ok",
        "runtime": "Python urllib + SparkSession ativa para materializar DataFrame",
        "python_packages": [],
        "notes": ["Alias de http_file com format=json."],
    },
    "http_text": {
        "status": "ok",
        "runtime": "Python urllib + SparkSession ativa para materializar DataFrame",
        "python_packages": [],
        "notes": ["Alias de http_file com format=text."],
    },
}

_STANDARD_CONNECTOR_STATUS = {
    "status": "ok",
    "runtime": "Spark reader/catalog padrão",
    "python_packages": [],
    "notes": [],
}


def _source_complete(spec: ConnectorSpec) -> bool:
    return bool(spec.read.get("source_complete", False) or spec.read.get("full_snapshot", False))


def _watermark_previous(plan: IngestionPlan) -> Optional[str]:
    value = (plan.runtime_parameters or {}).get("_contractforge_watermark_previous")
    return None if value is None else str(value)


def _incremental_watermark_column(spec: ConnectorSpec, plan: IngestionPlan) -> Optional[str]:
    column = spec.incremental.get("watermark_column") if spec.incremental else None
    if column not in {None, ""}:
        return str(column)
    if len(plan.watermark_columns) == 1:
        return plan.watermark_columns[0]
    return None


def _extract_incremental_watermark_value(
    raw: str,
    spec: ConnectorSpec,
    plan: IngestionPlan,
) -> str:
    text = raw.strip()
    if not text.startswith("{"):
        return raw
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return raw
    if not isinstance(parsed, dict):
        return raw

    column = _incremental_watermark_column(spec, plan)
    if not column:
        raise ValueError(
            "source.incremental com watermark tipado exige watermark_column "
            "quando o plano usa watermark composto"
        )
    item = parsed.get(column)
    if not isinstance(item, Mapping) or "value" not in item:
        raise ValueError(f"Watermark anterior não contém valor para source.incremental.watermark_column={column!r}")
    value = item.get("value")
    return "" if value is None else str(value)


def _incremental_watermark_value(spec: ConnectorSpec, plan: IngestionPlan) -> Optional[str]:
    previous = _watermark_previous(plan)
    if previous not in {None, ""}:
        return _extract_incremental_watermark_value(previous, spec, plan)
    initial = spec.incremental.get("initial_value") if spec.incremental else None
    return None if initial in {None, ""} else str(initial)


def _format_incremental_template(template: str, watermark_value: str) -> str:
    return str(template).format(watermark_previous=watermark_value, watermark=watermark_value)


def _spark_options(options: Mapping[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in options.items():
        if str(key) == "schema":
            continue
        if isinstance(value, bool):
            normalized[str(key)] = "true" if value else "false"
        else:
            normalized[str(key)] = str(value)
    return normalized


def _optional_source_schema(spec: ConnectorSpec) -> str:
    """Retorna schema Spark DDL declarado para fontes de arquivo."""
    raw = spec.read.get("schema") or spec.options.get("schema")
    if raw is None:
        return ""
    schema = str(raw).strip()
    if not schema:
        raise ValueError("source.read.schema não pode ser vazio")
    return schema


def _bool_option(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "sim"}:
        return True
    if text in {"0", "false", "no", "n", "nao", "não"}:
        return False
    raise ValueError(f"Valor booleano inválido: {value!r}")


def _request_headers(
    spec: ConnectorSpec,
    connector: str,
    oauth_token_getter: Optional[Any] = None,
) -> Dict[str, str]:
    request_headers = dict(spec.request.get("headers") or {})
    auth = resolve_secrets(spec.auth or {})
    auth_type = str(auth.get("type") or "").strip()
    if auth_type == "bearer_token":
        token = auth.get("token")
        if not token:
            raise ValueError("auth.token é obrigatório quando auth.type=bearer_token")
        request_headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "api_key":
        header_name = str(auth.get("header") or "x-api-key")
        key_value = auth.get("value") or auth.get("key")
        if not key_value:
            raise ValueError("auth.value ou auth.key é obrigatório quando auth.type=api_key")
        request_headers[header_name] = str(key_value)
    elif auth_type == "basic":
        if not auth.get("username") or not auth.get("password"):
            raise ValueError("auth.username e auth.password são obrigatórios quando auth.type=basic")
        raw = f"{auth.get('username')}:{auth.get('password')}".encode("utf-8")
        request_headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
    elif auth_type == "oauth_client_credentials":
        if oauth_token_getter is None:
            raise ValueError(f"auth.type='oauth_client_credentials' não suportado para connector={connector}")
        request_headers["Authorization"] = f"Bearer {oauth_token_getter(spec, auth)}"
    elif auth_type in {"", "none"}:
        pass
    else:
        raise ValueError(f"auth.type={auth_type!r} não suportado para connector={connector}")
    return {str(k): str(v) for k, v in resolve_secrets(request_headers).items()}


def _connector_metadata(spec: ConnectorSpec, capabilities: ConnectorCapabilities) -> Dict[str, Any]:
    return {
        "source_type": spec.type,
        "source_connector": spec.connector,
        "source_name": _redact_optional_text(spec.name),
        "source_provider": spec.provider,
        "source_format": spec.format,
        "source_path": _redact_optional_text(spec.path),
        "source_table": _redact_optional_text(spec.table),
        "source_query": bool(spec.query),
        "source_options_redacted": redact_secrets(spec.options),
        "source_read_redacted": redact_secrets(spec.read),
        "source_request_redacted": redact_secrets(spec.request),
        "source_auth_redacted": redact_secrets(spec.auth),
        "source_pagination_redacted": redact_secrets(spec.pagination),
        "source_response_redacted": redact_secrets(spec.response),
        "source_incremental_redacted": redact_secrets(spec.incremental),
        "source_limits_redacted": redact_secrets(spec.limits),
        "source_capabilities": asdict(capabilities),
        "source_metrics": {},
    }


def _redact_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return redact_text(str(value))


def redact_secrets(value: Any) -> Any:
    """Remove valores sensíveis de estruturas que podem ir para logs/ctrl tables."""
    if isinstance(value, Mapping):
        redacted = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(part in key_text for part in _SENSITIVE_KEY_PARTS):
                redacted[key] = _SECRET_MARKER
            else:
                redacted[key] = redact_secrets(item)
        return redacted
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item) for item in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_text(value: str) -> str:
    """Redige padrões sensíveis em texto livre antes de persistir auditoria."""
    stripped = value.strip()
    if stripped.startswith("{{ secret:") and stripped.endswith("}}"):
        return _SECRET_MARKER
    redacted = _SECRET_PLACEHOLDER_RE.sub(_SECRET_MARKER, value)
    redacted = _AUTH_HEADER_RE.sub(lambda match: f"{match.group(1)} {_SECRET_MARKER}", redacted)
    redacted = _URL_USERINFO_RE.sub(lambda match: f"{match.group(1)}{_SECRET_MARKER}:{_SECRET_MARKER}@", redacted)
    redacted = _SENSITIVE_PARAM_RE.sub(lambda match: f"{match.group(1)}{_SECRET_MARKER}", redacted)
    redacted = _SENSITIVE_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_SECRET_MARKER}",
        redacted,
    )
    return redacted


def _dbutils() -> Any:
    try:
        from IPython import get_ipython  # type: ignore

        shell = get_ipython()
        if shell and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except Exception as exc:
        raise RuntimeError("Não foi possível resolver dbutils para acessar Databricks Secrets") from exc


def _secret_from_placeholder(value: str) -> str:
    raw = value.strip()
    if not (raw.startswith("{{") and raw.endswith("}}")):
        return value
    token = raw[2:-2].strip()
    if not token.startswith("secret:"):
        return value
    ref = token[len("secret:"):].strip()
    if "/" not in ref:
        raise ValueError("Placeholder de secret deve usar formato {{ secret:scope/key }}")
    scope, key = [part.strip() for part in ref.split("/", 1)]
    if not scope or not key:
        raise ValueError("Placeholder de secret requer scope e key não vazios")
    env_name = f"CONTRACTFORGE_SECRET_{scope}_{key}".upper().replace("-", "_").replace(".", "_")
    if env_name in os.environ:
        return os.environ[env_name]
    return _dbutils().secrets.get(scope=scope, key=key)


def resolve_secrets(value: Any) -> Any:
    """Resolve placeholders ``{{ secret:scope/key }}`` recursivamente."""
    if isinstance(value, Mapping):
        return {key: resolve_secrets(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(resolve_secrets(item) for item in value)
    if isinstance(value, str):
        return _secret_from_placeholder(value)
    return value


def register_source_resolver(source_type: str, resolver: SourceResolver, *, overwrite: bool = False) -> None:
    """Registra resolver declarativo por ``source.type`` ou ``source.connector``."""
    normalized = str(source_type or "").strip()
    if not _CONNECTOR_NAME_RE.match(normalized):
        raise ValueError("source_type deve começar por letra e conter apenas letras, números, '_' ou '-'")
    if not hasattr(resolver, "resolve_stream") and not hasattr(resolver, "resolve_batch"):
        raise ValueError("resolver deve implementar resolve_batch(spec, plan) ou resolve_stream(spec, plan)")
    if normalized in SOURCE_RESOLVER_REGISTRY and not overwrite:
        raise ValueError(f"source resolver já registrado: {normalized}")
    SOURCE_RESOLVER_REGISTRY[normalized] = resolver


def get_source_resolver(source_type: str) -> SourceResolver:
    """Retorna resolver registrado para ``source_type``."""
    normalized = str(source_type or "").strip()
    resolver = SOURCE_RESOLVER_REGISTRY.get(normalized)
    if resolver is None:
        raise ValueError(f"source.type={normalized!r} não tem resolver registrado")
    return resolver


def list_source_resolvers() -> list[str]:
    """Lista resolvers registrados."""
    return sorted(SOURCE_RESOLVER_REGISTRY)


def source_connector_details(name: str) -> Dict[str, Any]:
    """Retorna metadata e capabilities do conector registrado."""
    normalized = str(name or "").strip()
    resolver = get_source_resolver(normalized)
    spec = ConnectorSpec(connector=normalized)
    capabilities = resolver.capabilities(spec) if hasattr(resolver, "capabilities") else ConnectorCapabilities()
    builtin = BUILTIN_CONNECTOR_METADATA.get(normalized, {})
    return {
        "name": normalized,
        "registered": True,
        "builtin": normalized in BUILTIN_CONNECTOR_METADATA,
        "family": builtin.get("family", "custom"),
        "description": builtin.get("description"),
        "required": builtin.get("required", []),
        "incremental": builtin.get("incremental", False),
        "capabilities": asdict(capabilities),
    }


def list_source_connector_details() -> list[Dict[str, Any]]:
    """Lista metadata/capabilities de todos os conectores registrados."""
    return [source_connector_details(name) for name in list_source_resolvers()]


def diagnose_source_connectors(names: Optional[Iterable[str]] = None) -> list[Dict[str, Any]]:
    """Diagnostica requisitos estáticos dos conectores sem abrir Spark/conexões."""
    connector_names = list(names) if names is not None else list_source_resolvers()
    diagnostics = []
    for raw_name in connector_names:
        name = str(raw_name or "").strip()
        details = source_connector_details(name)
        requirements = dict(CONNECTOR_RUNTIME_REQUIREMENTS.get(name, _STANDARD_CONNECTOR_STATUS))
        python_packages = list(requirements.get("python_packages") or [])
        missing_packages = [
            package for package in python_packages if importlib.util.find_spec(package.replace("-", "_")) is None
        ]
        status = str(requirements.get("status") or "ok")
        if missing_packages:
            status = "missing_python_package"
        if name not in BUILTIN_CONNECTOR_METADATA:
            status = "custom"
            requirements.setdefault("runtime", "Conector customizado registrado pela aplicação")
            requirements.setdefault("notes", ["Validação depende do resolver customizado."])
        diagnostics.append(
            {
                "name": name,
                "status": status,
                "family": details.get("family"),
                "registered": True,
                "builtin": name in BUILTIN_CONNECTOR_METADATA,
                "capabilities": details.get("capabilities", {}),
                "runtime": requirements.get("runtime"),
                "python_packages": python_packages,
                "missing_python_packages": missing_packages,
                "notes": list(requirements.get("notes") or []),
            }
        )
    return diagnostics


def resolve_batch_source(spec: ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
    """Resolve ``ConnectorSpec`` como batch source."""
    resolver = get_source_resolver(spec.connector)
    if not hasattr(resolver, "resolve_batch"):
        raise ValueError(f"source.connector={spec.connector!r} não suporta leitura batch")
    return resolver.resolve_batch(spec, plan)


class AutoloaderResolver:
    """Resolver Databricks Auto Loader (`cloudFiles`) em modo available_now."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        return ConnectorCapabilities(batch=False, streaming=True, incremental_read=True)

    def resolve_stream(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> Tuple[DataFrame, str]:
        if isinstance(spec, ConnectorSpec):
            spec = SourceSpec(
                type="autoloader",
                path=str(spec.path or ""),
                format=str(spec.format or "parquet"),
                schema_location=str(spec.read.get("schema_location") or spec.options.get("cloudFiles.schemaLocation") or ""),
                checkpoint_location=str(spec.read.get("checkpoint_location") or ""),
                options=spec.options,
                schema_hints=spec.read.get("schema_hints"),
                include_existing_files=bool(spec.read.get("include_existing_files", True)),
                max_files_per_trigger=spec.read.get("max_files_per_trigger"),
            )
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", spec.format)
            .option("cloudFiles.schemaLocation", spec.schema_location)
            .option("cloudFiles.includeExistingFiles", "true" if spec.include_existing_files else "false")
            .options(**(spec.options or {}))
        )
        if spec.schema_hints:
            reader = reader.option("cloudFiles.schemaHints", spec.schema_hints)
        if spec.max_files_per_trigger is not None:
            reader = reader.option("cloudFiles.maxFilesPerTrigger", str(spec.max_files_per_trigger))
        return reader.load(spec.path), f"autoloader:{spec.path}"


class TableConnector:
    """Lê tabela ou view registrada no catálogo Spark/Unity Catalog."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else True
        return ConnectorCapabilities(batch=True, pushdown_filter=True, source_complete=source_complete)

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("TableConnector requer ConnectorSpec")
        table = spec.table or spec.path or spec.options.get("table")
        if not table:
            raise ValueError("source.table é obrigatório para connector=table/delta_table")
        capabilities = self.capabilities(spec)
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "spark_table",
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            spark.read.table(str(table)),
            redact_text(f"{spec.connector}:{table}"),
            spec.connector,
            metadata,
            capabilities,
        )


class SqlConnector:
    """Executa SQL declarativo e retorna DataFrame."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(batch=True, pushdown_filter=False, source_complete=source_complete)

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("SqlConnector requer ConnectorSpec")
        query = spec.query or spec.options.get("query")
        if not query:
            raise ValueError("source.query é obrigatório para connector=sql")
        capabilities = self.capabilities(spec)
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "spark_sql",
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            spark.sql(str(query)),
            f"sql:{spec.name or 'query'}",
            spec.connector,
            metadata,
            capabilities,
        )


class FileConnector:
    """Lê arquivos por path usando ``spark.read.format(...).load``."""

    def __init__(self, default_format: Optional[str] = None) -> None:
        self.default_format = default_format

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(
            batch=True,
            schema_inference=True,
            source_complete=source_complete,
        )

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("FileConnector requer ConnectorSpec")
        fmt = (spec.format or self.default_format or spec.connector).strip()
        if fmt not in VALID_FILE_CONNECTOR_FORMATS:
            raise ValueError(f"Formato de arquivo não suportado: {fmt}. Válidos: {sorted(VALID_FILE_CONNECTOR_FORMATS)}")
        path = spec.path or spec.options.get("path")
        if not path:
            raise ValueError(f"source.path é obrigatório para connector={spec.connector}")
        options = resolve_secrets(spec.options)
        schema = _optional_source_schema(spec)
        capabilities = self.capabilities(spec)
        reader = spark.read.format(_spark_file_format(fmt)).options(**_spark_options(options))
        if schema:
            reader = reader.schema(schema)
        df = reader.load(str(path))
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "spark_files",
            "file_format": fmt,
            "source_complete": capabilities.source_complete,
            "schema_declared": bool(schema),
        }
        return SourceResolution(
            df,
            redact_text(f"{spec.connector}:{path}"),
            spec.connector,
            metadata,
            capabilities,
        )


class ObjectStorageConnector(FileConnector):
    """Lê arquivos em object storage: ADLS/Azure Blob/S3/GCS."""

    def __init__(self, default_provider: Optional[str] = None) -> None:
        super().__init__()
        self.default_provider = default_provider

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("ObjectStorageConnector requer ConnectorSpec")
        explicit_provider = str(spec.provider or "").strip()
        if explicit_provider and self.default_provider and explicit_provider != self.default_provider:
            raise ValueError(
                f"source.provider={explicit_provider!r} conflita com connector={spec.connector!r}; "
                f"use provider={self.default_provider!r} ou remova provider"
            )
        provider = str(explicit_provider or self.default_provider or "").strip()
        if provider and provider not in VALID_OBJECT_STORAGE_PROVIDERS:
            raise ValueError(
                f"source.provider={provider!r} não é suportado. "
                f"Valores válidos: {sorted(VALID_OBJECT_STORAGE_PROVIDERS)}"
            )
        if not spec.format:
            raise ValueError(f"source.format é obrigatório para connector={spec.connector}")
        fmt = (spec.format or self.default_format or spec.connector).strip()
        if fmt not in VALID_FILE_CONNECTOR_FORMATS:
            raise ValueError(f"Formato de arquivo não suportado: {fmt}. Válidos: {sorted(VALID_FILE_CONNECTOR_FORMATS)}")
        try:
            path, options, storage_metrics = self._resolve_storage_path_and_options(spec, provider)
        except Exception as exc:
            if provider == "azure_blob" and self._is_spark_config_blocked(exc):
                raise RuntimeError(
                    "Databricks serverless/Spark Connect bloqueou a configuração de SAS no Spark para "
                    f"connector=azure_blob e format={fmt!r}. Em serverless, use Unity Catalog External "
                    "Location/Volume com path abfss:// ou /Volumes/..., ou configure Serverless Network "
                    "Policy/NCC para permitir o destino. Use SAS direto apenas em job cluster/classic/local "
                    "onde Hadoop config fs.azure.sas.* é permitido."
                ) from exc
            raise
        capabilities = self.capabilities(spec)
        schema = _optional_source_schema(spec)
        reader = spark.read.format(_spark_file_format(fmt)).options(**_spark_options(options))
        if schema:
            reader = reader.schema(schema)
        df = reader.load(str(path))
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "spark_files",
            "file_format": fmt,
            "source_complete": capabilities.source_complete,
            "schema_declared": bool(schema),
            **storage_metrics,
        }
        resolved = SourceResolution(
            df,
            redact_text(f"{spec.connector}:{path}"),
            spec.connector,
            metadata,
            capabilities,
        )
        resolved.metadata["source_provider"] = provider or spec.provider
        resolved.metadata["source_metrics"]["object_storage_provider"] = provider or spec.provider
        return resolved

    def _resolve_storage_path_and_options(
        self,
        spec: ConnectorSpec,
        provider: str,
    ) -> tuple[str, Mapping[str, Any], dict[str, Any]]:
        options = resolve_secrets(spec.options)
        raw_path = spec.path or options.get("path")
        if not raw_path:
            raise ValueError(f"source.path é obrigatório para connector={spec.connector}")
        path = str(raw_path)
        metrics: dict[str, Any] = {}
        if provider == "azure_blob":
            path = self._resolve_azure_blob_path(spec, path)
            metrics["azure_auth_configured"] = bool((spec.auth or {}).get("sas_token") or (spec.auth or {}).get("token"))
            metrics["azure_container"] = spec.container or self._azure_container_from_uri(path)
        return path, options, metrics

    def _resolve_azure_blob_path(self, spec: ConnectorSpec, path: str) -> str:
        auth = resolve_secrets(spec.auth or {})
        sas_token = auth.get("sas_token") or auth.get("token")
        if spec.account_url or spec.container:
            account = self._azure_account_from_url(spec.account_url or "")
            container = str(spec.container or "").strip()
            if not account:
                raise ValueError("source.account_url é obrigatório para connector=azure_blob quando source.container é usado")
            if not container:
                raise ValueError("source.container é obrigatório para connector=azure_blob quando source.account_url é usado")
            if sas_token:
                self._configure_azure_blob_sas(account, container, str(sas_token))
            if "://" not in path:
                normalized_path = path.lstrip("/")
                return f"wasbs://{container}@{account}.blob.core.windows.net/{normalized_path}"
            return path
        if sas_token:
            account, container = self._azure_account_container_from_uri(path)
            if not account or not container:
                raise ValueError(
                    "auth.sas_token em connector=azure_blob requer source.account_url/source.container "
                    "ou path wasbs://container@account.blob.core.windows.net/..."
                )
            self._configure_azure_blob_sas(account, container, str(sas_token))
        return path

    @staticmethod
    def _azure_account_from_url(account_url: str) -> str:
        if not account_url:
            return ""
        parsed = urllib.parse.urlparse(account_url if "://" in account_url else f"https://{account_url}")
        host = parsed.netloc or parsed.path
        return host.split(".", 1)[0].strip()

    @classmethod
    def _azure_account_container_from_uri(cls, path: str) -> tuple[str, str]:
        parsed = urllib.parse.urlparse(path)
        if parsed.scheme not in {"wasbs", "wasb", "abfss", "abfs"}:
            return "", ""
        netloc = parsed.netloc
        if "@" not in netloc:
            return "", ""
        container, host = netloc.split("@", 1)
        account = host.split(".", 1)[0].strip()
        return account, container.strip()

    @classmethod
    def _azure_container_from_uri(cls, path: str) -> Optional[str]:
        _, container = cls._azure_account_container_from_uri(path)
        return container or None

    @staticmethod
    def _configure_azure_blob_sas(account: str, container: str, sas_token: str) -> None:
        token = sas_token.strip()
        if token.startswith("?"):
            token = token[1:]
        if not token:
            raise ValueError("auth.sas_token não pode ser vazio para connector=azure_blob")
        spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.windows.net", token)

    @staticmethod
    def _is_spark_config_blocked(exc: Exception) -> bool:
        message = str(exc)
        return "CONFIG_NOT_AVAILABLE" in message or "Configuration fs.azure.sas" in message

class SparkFormatConnector:
    """Lê fontes externas por ``spark.read.format`` quando o runtime já possui o conector Spark."""

    def __init__(self, spark_format: str, *, table_option: str = "table") -> None:
        self.spark_format = spark_format
        self.table_option = table_option

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(
            batch=True,
            pushdown_filter=True,
            schema_inference=True,
            requires_secrets=True,
            source_complete=source_complete,
        )

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("SparkFormatConnector requer ConnectorSpec")
        options = {str(k): v for k, v in resolve_secrets(spec.options).items()}
        if spec.table and "table" not in options and "dbtable" not in options and "query" not in options:
            options[self.table_option] = spec.table
        if spec.query and "query" not in options:
            options["query"] = spec.query
        if "table" not in options and "dbtable" not in options and "query" not in options:
            raise ValueError(
                f"connector={spec.connector} requer source.table, source.query, "
                "source.options.table, source.options.dbtable ou source.options.query"
            )
        capabilities = self.capabilities(spec)
        df = spark.read.format(self.spark_format).options(**_spark_options(options)).load()
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "spark_format",
            "spark_format": self.spark_format,
            "source_table": _redact_optional_text(options.get("table") or options.get("dbtable") or spec.table),
            "source_query": bool(options.get("query")),
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            df,
            redact_text(f"{spec.connector}:{spec.name or options.get('table') or options.get('dbtable') or 'query'}"),
            spec.connector,
            metadata,
            capabilities,
        )


class JdbcConnector:
    """Lê fonte JDBC usando Spark JDBC."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        partitioned = isinstance(spec, ConnectorSpec) and bool(spec.read.get("partition_column"))
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(
            batch=True,
            pushdown_filter=True,
            partitioned_read=partitioned,
            schema_inference=True,
            requires_secrets=True,
            source_complete=source_complete,
        )

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("JdbcConnector requer ConnectorSpec")
        options = {str(k): v for k, v in resolve_secrets(spec.options).items()}
        if "url" not in options:
            raise ValueError(f"source.options.url é obrigatório para connector={spec.connector}")
        if "dbtable" not in options and "query" not in options:
            raise ValueError(f"connector={spec.connector} requer source.options.dbtable ou source.options.query")
        watermark_value = _incremental_watermark_value(spec, plan)
        if watermark_value:
            self._apply_incremental_predicate(spec, options, watermark_value)
        read = spec.read or {}
        partition_fields = ["partition_column", "lower_bound", "upper_bound", "num_partitions"]
        provided = [field for field in partition_fields if read.get(field) is not None]
        if provided and len(provided) != len(partition_fields):
            raise ValueError(
                "JDBC partitioning requer partition_column, lower_bound, upper_bound e num_partitions juntos"
            )
        if provided:
            options.update(
                {
                    "partitionColumn": str(read["partition_column"]),
                    "lowerBound": str(read["lower_bound"]),
                    "upperBound": str(read["upper_bound"]),
                    "numPartitions": str(read["num_partitions"]),
                }
            )
        if read.get("fetchsize") is not None:
            options["fetchsize"] = str(read["fetchsize"])
        capabilities = self.capabilities(spec)
        df = spark.read.format("jdbc").options(**_spark_options(options)).load()
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "jdbc_query" if "query" in spec.options else "jdbc_table",
            "incremental_applied": watermark_value is not None,
            "watermark_value": watermark_value,
            "partitioned_read": bool(provided),
            "fetchsize": read.get("fetchsize"),
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            df,
            redact_text(f"{spec.connector}:{spec.name or options.get('dbtable') or 'query'}"),
            spec.connector,
            metadata,
            capabilities,
        )

    def _apply_incremental_predicate(
        self,
        spec: ConnectorSpec,
        options: Dict[str, Any],
        watermark_value: str,
    ) -> None:
        incremental = spec.incremental or {}
        predicate_template = incremental.get("predicate")
        watermark_column = incremental.get("watermark_column")
        if predicate_template:
            predicate = _format_incremental_template(str(predicate_template), watermark_value)
        elif watermark_column:
            predicate = f"{watermark_column} > '{watermark_value}'"
        else:
            return
        alias = str(incremental.get("alias") or "cf_src")
        if "query" in options:
            query = str(options.pop("query"))
            options["dbtable"] = f"(SELECT * FROM ({query}) {alias} WHERE {predicate}) {alias}"
        else:
            dbtable = str(options["dbtable"])
            options["dbtable"] = f"(SELECT * FROM {dbtable} WHERE {predicate}) {alias}"


def _json_path(payload: Any, path: Optional[str]) -> Any:
    if not path or path == "$":
        return payload
    raw = path.strip()
    if not raw.startswith("$."):
        raise ValueError(f"JSON path simples esperado no formato $.campo.subcampo: {path}")
    current = payload
    for part in raw[2:].split("."):
        if isinstance(current, Mapping):
            current = current.get(part)
        else:
            return None
    return current


def _records_from_response(payload: Any, records_path: Optional[str]) -> list[Any]:
    records = _json_path(payload, records_path or "$")
    if records is None:
        return []
    if isinstance(records, list):
        return records
    return [records]


def _link_header_next(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    for chunk in value.split(","):
        url_part, *attrs = chunk.split(";")
        if any('rel="next"' in attr.strip() or "rel=next" in attr.strip() for attr in attrs):
            return url_part.strip().strip("<>")
    return None


class HttpFileConnector:
    """Baixa arquivos HTTP(S) pelo driver Python e materializa registros em DataFrame Spark."""

    def __init__(self, default_format: Optional[str] = None) -> None:
        self.default_format = default_format

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(
            batch=True,
            schema_inference=True,
            requires_secrets=True,
            source_complete=source_complete,
        )

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("HttpFileConnector requer ConnectorSpec")
        request = resolve_secrets(spec.request or {})
        url = str(request.get("url") or spec.path or "").strip()
        if not url:
            raise ValueError(f"source.path ou source.request.url é obrigatório para connector={spec.connector}")
        method = str(request.get("method") or "GET").upper()
        if method != "GET":
            raise ValueError(f"connector={spec.connector} suporta apenas HTTP GET")
        fmt = str(
            spec.format
            or spec.response.get("format")
            or spec.options.get("format")
            or self.default_format
            or ""
        ).strip().lower()
        if not fmt:
            raise ValueError("source.format é obrigatório para connector=http_file")
        if fmt not in VALID_HTTP_FILE_FORMATS:
            raise ValueError(f"Formato HTTP não suportado: {fmt}. Válidos: {sorted(VALID_HTTP_FILE_FORMATS)}")

        params = resolve_secrets(request.get("params") or {})
        if params:
            if not isinstance(params, Mapping):
                raise ValueError("source.request.params deve ser objeto")
            for key, value in params.items():
                url = _with_query_param(url, str(key), str(value))
        timeout = int(spec.limits.get("timeout_seconds") or 60)
        retry_attempts = int(spec.limits.get("retry_attempts") or 3)
        backoff = float(spec.limits.get("retry_backoff_seconds") or 1)
        headers = _request_headers(spec, spec.connector)

        raw, response_headers, final_url, bytes_read = self._request_with_retry(
            url, headers, timeout, retry_attempts, backoff
        )
        encoding = str(spec.response.get("encoding") or spec.options.get("encoding") or "").strip()
        if not encoding:
            encoding = response_headers.get_content_charset() if hasattr(response_headers, "get_content_charset") else None
        text = raw.decode(encoding or "utf-8-sig")
        records = self._parse_records(text, fmt, spec)
        df = _records_to_dataframe(spark, records)

        capabilities = self.capabilities(spec)
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_format"] = fmt
        metadata["source_metrics"] = {
            "read_strategy": "http_file",
            "file_format": fmt,
            "records_read": len(records),
            "bytes_read": bytes_read,
            "retry_attempts": retry_attempts,
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            df,
            redact_text(f"{spec.connector}:{spec.name or urllib.parse.urlparse(final_url).netloc}"),
            spec.connector,
            metadata,
            capabilities,
        )

    def _request(self, url: str, headers: Mapping[str, str], timeout: int) -> tuple[bytes, Mapping[str, str], str, int]:
        request = urllib.request.Request(url=url, method="GET", headers=dict(headers))
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return raw, response.headers, response.geturl(), len(raw)

    def _request_with_retry(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: int,
        attempts: int,
        backoff: float,
    ) -> tuple[bytes, Mapping[str, str], str, int]:
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                return self._request(url, headers, timeout)
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code < 500 and exc.code != 429:
                    raise
            except Exception as exc:
                last_error = exc
            if attempt < attempts:
                time.sleep(backoff * attempt)
        assert last_error is not None
        raise last_error

    def _parse_records(self, text: str, fmt: str, spec: ConnectorSpec) -> list[Any]:
        if fmt == "csv":
            return self._parse_csv(text, spec)
        if fmt == "json":
            payload = json.loads(text) if text.strip() else []
            records_path = spec.response.get("records_path") if spec.response else None
            return _records_from_response(payload, records_path)
        if fmt in _JSON_LINES_FORMATS:
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        if fmt == "text":
            return [{"value": line} for line in text.splitlines()]
        raise ValueError(f"Formato HTTP não suportado: {fmt}. Válidos: {sorted(VALID_HTTP_FILE_FORMATS)}")

    def _parse_csv(self, text: str, spec: ConnectorSpec) -> list[dict[str, Any]]:
        options = spec.options or {}
        delimiter = str(options.get("delimiter") or options.get("sep") or ",")
        quotechar = str(options.get("quote") or '"')
        escapechar = options.get("escape")
        null_value = options.get("nullValue")
        header = _bool_option(options.get("header"), default=False)
        reader_options = {
            "delimiter": delimiter,
            "quotechar": quotechar,
            "escapechar": (str(escapechar) if escapechar not in {None, ""} else None),
        }
        stream = io.StringIO(text)
        records: list[dict[str, Any]] = []
        if header:
            for row in csv.DictReader(stream, **reader_options):
                records.append({str(key): self._normalize_csv_value(value, null_value) for key, value in row.items()})
            return records
        for row in csv.reader(stream, **reader_options):
            records.append({f"_c{idx}": self._normalize_csv_value(value, null_value) for idx, value in enumerate(row)})
        return records

    @staticmethod
    def _normalize_csv_value(value: Any, null_value: Any) -> Any:
        if value is None:
            return None
        if null_value is not None and str(value) == str(null_value):
            return None
        return value


class RestApiConnector:
    """Conector REST API batch com paginação básica e resposta JSON."""

    def capabilities(self, spec: SourceSpec | ConnectorSpec) -> ConnectorCapabilities:
        source_complete = _source_complete(spec) if isinstance(spec, ConnectorSpec) else False
        return ConnectorCapabilities(
            batch=True,
            incremental_read=isinstance(spec, ConnectorSpec) and bool(spec.incremental),
            schema_inference=True,
            requires_secrets=True,
            source_complete=source_complete,
        )

    def _request(
        self,
        url: str,
        method: str,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout: int,
        *,
        parse_json_payload: bool,
    ) -> tuple[Any, Mapping[str, str], str, int, str]:
        request = urllib.request.Request(url=url, method=method, headers=dict(headers), data=body)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            text = raw.decode(response.headers.get_content_charset() or "utf-8")
            payload = json.loads(text) if parse_json_payload and text else None
            return payload, response.headers, response.geturl(), len(raw), text

    def _headers(self, spec: ConnectorSpec) -> Dict[str, str]:
        return _request_headers(spec, "rest_api", self._oauth_client_credentials_token)

    def _oauth_client_credentials_token(self, spec: ConnectorSpec, auth: Mapping[str, Any]) -> str:
        token_url = str(auth.get("token_url") or "").strip()
        client_id = auth.get("client_id")
        client_secret = auth.get("client_secret")
        if not token_url or not client_id or not client_secret:
            raise ValueError(
                "auth.token_url, auth.client_id e auth.client_secret são obrigatórios "
                "quando auth.type=oauth_client_credentials"
            )
        fields = {
            "grant_type": "client_credentials",
            "client_id": str(client_id),
            "client_secret": str(client_secret),
        }
        if auth.get("scope"):
            fields["scope"] = str(auth["scope"])
        timeout = int(spec.limits.get("timeout_seconds") or 60)
        body = urllib.parse.urlencode(fields).encode("utf-8")
        request = urllib.request.Request(
            token_url,
            method="POST",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode(response.headers.get_content_charset() or "utf-8"))
        token = payload.get("access_token")
        if not token:
            raise ValueError("Resposta OAuth não retornou access_token")
        return str(token)

    def _body(self, spec: ConnectorSpec) -> Optional[bytes]:
        if "json" in spec.request:
            return json.dumps(resolve_secrets(spec.request["json"])).encode("utf-8")
        if "body" in spec.request:
            value = resolve_secrets(spec.request["body"])
            return value.encode("utf-8") if isinstance(value, str) else bytes(value)
        return None

    def _body_with_incremental(self, spec: ConnectorSpec, watermark_value: Optional[str]) -> Optional[bytes]:
        if not watermark_value or not spec.incremental.get("watermark_body_field"):
            return self._body(spec)
        if "json" not in spec.request:
            raise ValueError("source.incremental.watermark_body_field exige source.request.json")
        payload = resolve_secrets(spec.request["json"])
        if not isinstance(payload, Mapping):
            raise ValueError("source.request.json deve ser objeto quando usar watermark_body_field")
        body_payload = dict(payload)
        body_payload[str(spec.incremental["watermark_body_field"])] = watermark_value
        return json.dumps(body_payload).encode("utf-8")

    def _page_urls(self, spec: ConnectorSpec, base_url: str) -> Iterable[str]:
        pagination = spec.pagination or {}
        page_type = str(pagination.get("type") or "none")
        max_pages = int(spec.limits.get("max_pages") or pagination.get("max_pages") or 1)
        if page_type == "none":
            yield base_url
            return
        if page_type == "page":
            page_param = str(pagination.get("page_param") or "page")
            start_page = int(pagination.get("start_page") or 1)
            for page in range(start_page, start_page + max_pages):
                yield _with_query_param(base_url, page_param, str(page))
            return
        if page_type == "offset":
            offset_param = str(pagination.get("offset_param") or "offset")
            limit_param = str(pagination.get("limit_param") or "limit")
            page_size = int(pagination.get("page_size") or 100)
            for idx in range(max_pages):
                url = _with_query_param(base_url, offset_param, str(idx * page_size))
                yield _with_query_param(url, limit_param, str(page_size))
            return
        if page_type in {"cursor", "link_header"}:
            yield base_url
            return
        raise ValueError(f"pagination.type={page_type!r} não suportado")

    def resolve_batch(self, spec: SourceSpec | ConnectorSpec, plan: IngestionPlan) -> SourceResolution:
        if not isinstance(spec, ConnectorSpec):
            raise ValueError("RestApiConnector requer ConnectorSpec")
        request = resolve_secrets(spec.request or {})
        url = str(request.get("url") or spec.path or "").strip()
        if not url:
            raise ValueError("source.request.url é obrigatório para connector=rest_api")
        params = resolve_secrets(request.get("params") or {})
        if params:
            if not isinstance(params, Mapping):
                raise ValueError("source.request.params deve ser objeto")
            for key, value in params.items():
                url = _with_query_param(url, str(key), str(value))
        watermark_value = _incremental_watermark_value(spec, plan)
        if watermark_value and spec.incremental.get("watermark_param"):
            url = _with_query_param(url, str(spec.incremental["watermark_param"]), watermark_value)
        method = str(request.get("method") or "GET").upper()
        if method not in {"GET", "POST"}:
            raise ValueError("connector=rest_api suporta apenas GET e POST")
        timeout = int(spec.limits.get("timeout_seconds") or 60)
        retry_attempts = int(spec.limits.get("retry_attempts") or 3)
        backoff = float(spec.limits.get("retry_backoff_seconds") or 1)
        rate_limit_per_minute = int(spec.limits.get("rate_limit_per_minute") or 0)
        max_page_bytes = int(spec.limits.get("max_page_bytes") or 0)
        max_total_bytes = int(spec.limits.get("max_total_bytes") or 0)
        min_request_interval = 60.0 / rate_limit_per_minute if rate_limit_per_minute > 0 else 0.0
        last_request_at = 0.0
        response_mode = str(spec.response.get("mode") or "records").strip().lower()
        raw_column = str(spec.response.get("raw_column") or "raw_response").strip()
        records_path = spec.response.get("records_path") if spec.response else None
        if response_mode not in {"records", "raw"}:
            raise ValueError("source.response.mode deve ser 'records' ou 'raw'")
        if response_mode == "raw" and records_path:
            raise ValueError("source.response.records_path não deve ser usado quando response.mode=raw")
        if response_mode == "raw" and not raw_column:
            raise ValueError("source.response.raw_column não pode ser vazio quando response.mode=raw")
        if response_mode == "raw" and not _SIMPLE_COLUMN_RE.match(raw_column):
            raise ValueError("source.response.raw_column deve ser um nome de coluna simples")
        if max_page_bytes < 0 or max_total_bytes < 0:
            raise ValueError("limits.max_page_bytes e limits.max_total_bytes devem ser positivos")
        headers = self._headers(spec)
        if watermark_value and spec.incremental.get("watermark_header"):
            headers[str(spec.incremental["watermark_header"])] = watermark_value
        body = self._body_with_incremental(spec, watermark_value)
        page_type = str((spec.pagination or {}).get("type") or "none")
        max_pages = int(spec.limits.get("max_pages") or spec.pagination.get("max_pages") or 1)
        all_records: list[Any] = []
        raw_rows: list[dict[str, Any]] = []
        next_url: Optional[str] = None
        bytes_read = 0
        parse_json_payload = response_mode == "records" or page_type == "cursor"

        pages = 0
        while True:
            if page_type in {"cursor", "link_header"}:
                if pages >= max_pages:
                    break
                url_candidates = [next_url or url]
            else:
                url_candidates = list(self._page_urls(spec, url))
            if not url_candidates:
                break
            for url_candidate in url_candidates:
                if pages >= max_pages:
                    break
                current_url = url_candidate
                if min_request_interval and last_request_at:
                    elapsed = time.monotonic() - last_request_at
                    if elapsed < min_request_interval:
                        time.sleep(min_request_interval - elapsed)
                payload, response_headers, final_url, response_bytes, response_text = self._request_with_retry(
                    current_url,
                    method,
                    headers,
                    body,
                    timeout,
                    retry_attempts,
                    backoff,
                    parse_json_payload=parse_json_payload,
                )
                if max_page_bytes > 0 and response_bytes > max_page_bytes:
                    raise ValueError(
                        "rest_api response excedeu limits.max_page_bytes: "
                        f"{response_bytes} > {max_page_bytes}. "
                        "Use paginação, reduza o range ou faça landing em storage + Auto Loader."
                    )
                last_request_at = time.monotonic()
                pages += 1
                bytes_read += response_bytes
                if max_total_bytes > 0 and bytes_read > max_total_bytes:
                    raise ValueError(
                        "rest_api response excedeu limits.max_total_bytes: "
                        f"{bytes_read} > {max_total_bytes}. "
                        "Use paginação, reduza o range ou faça landing em storage + Auto Loader."
                    )
                if response_mode == "raw":
                    raw_rows.append({raw_column: response_text, "response_page_number": pages})
                    records = raw_rows[-1:]
                else:
                    records = _records_from_response(payload, records_path)
                    all_records.extend(records)
                if page_type == "cursor":
                    cursor = _json_path(payload, spec.pagination.get("next_cursor_path"))
                    if not cursor:
                        next_url = None
                        break
                    cursor_param = str(spec.pagination.get("cursor_param") or "cursor")
                    next_url = _with_query_param(url, cursor_param, str(cursor))
                    break
                if page_type == "link_header":
                    next_url = _link_header_next(response_headers.get("Link"))
                    if not next_url or next_url == final_url:
                        next_url = None
                    break
                if page_type in {"page", "offset"} and not records:
                    next_url = None
                    break
            if page_type not in {"cursor", "link_header"} or not next_url:
                break

        df = _records_to_dataframe(spark, raw_rows if response_mode == "raw" else all_records)
        capabilities = self.capabilities(spec)
        metadata = _connector_metadata(spec, capabilities)
        metadata["source_metrics"] = {
            "read_strategy": "rest_api",
            "response_mode": response_mode,
            "request_count": pages,
            "pages_read": pages,
            "records_read": len(raw_rows) if response_mode == "raw" else len(all_records),
            "raw_payloads_read": len(raw_rows) if response_mode == "raw" else 0,
            "bytes_read": bytes_read,
            "max_page_bytes": max_page_bytes,
            "max_total_bytes": max_total_bytes,
            "pagination_type": page_type,
            "incremental_applied": watermark_value is not None,
            "watermark_value": watermark_value,
            "rate_limit_per_minute": rate_limit_per_minute,
            "retry_attempts": retry_attempts,
            "source_complete": capabilities.source_complete,
        }
        return SourceResolution(
            df,
            redact_text(f"rest_api:{spec.name or urllib.parse.urlparse(url).netloc}"),
            spec.connector,
            metadata,
            capabilities,
        )

    def _request_with_retry(
        self,
        url: str,
        method: str,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout: int,
        attempts: int,
        backoff: float,
        *,
        parse_json_payload: bool,
    ) -> tuple[Any, Mapping[str, str], str, int, str]:
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                return self._request(
                    url,
                    method,
                    headers,
                    body,
                    timeout,
                    parse_json_payload=parse_json_payload,
                )
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code < 500 and exc.code != 429:
                    raise
            except Exception as exc:
                last_error = exc
            if attempt < attempts:
                time.sleep(backoff * attempt)
        assert last_error is not None
        raise last_error


def _with_query_param(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    query[key] = value
    return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))


def _records_to_dataframe(session: SparkSession, records: list[Any]) -> DataFrame:
    if not records:
        return session.createDataFrame([], "value string").limit(0)
    normalized = [record if isinstance(record, Mapping) else {"value": record} for record in records]
    return session.createDataFrame(normalized)


def _spark_file_format(fmt: str) -> str:
    """Map ContractForge logical file formats to Spark reader names."""
    return "json" if fmt in _JSON_LINES_FORMATS else fmt


register_source_resolver("autoloader", AutoloaderResolver())
register_source_resolver("table", TableConnector())
register_source_resolver("delta_table", TableConnector())
register_source_resolver("view", TableConnector())
register_source_resolver("sql", SqlConnector())
register_source_resolver("parquet", FileConnector("parquet"))
register_source_resolver("delta", FileConnector("delta"))
register_source_resolver("json", FileConnector("json"))
register_source_resolver("csv", FileConnector("csv"))
register_source_resolver("orc", FileConnector("orc"))
register_source_resolver("text", FileConnector("text"))
register_source_resolver("http_file", HttpFileConnector())
register_source_resolver("http_csv", HttpFileConnector("csv"))
register_source_resolver("http_json", HttpFileConnector("json"))
register_source_resolver("http_text", HttpFileConnector("text"))
register_source_resolver("object_storage", ObjectStorageConnector())
register_source_resolver("blob", ObjectStorageConnector())
register_source_resolver("s3", ObjectStorageConnector("s3"))
register_source_resolver("adls", ObjectStorageConnector("adls"))
register_source_resolver("azure_blob", ObjectStorageConnector("azure_blob"))
register_source_resolver("gcs", ObjectStorageConnector("gcs"))
register_source_resolver("jdbc", JdbcConnector())
register_source_resolver("postgres", JdbcConnector())
register_source_resolver("postgresql", JdbcConnector())
register_source_resolver("sqlserver", JdbcConnector())
register_source_resolver("mysql", JdbcConnector())
register_source_resolver("oracle", JdbcConnector())
register_source_resolver("snowflake", SparkFormatConnector("snowflake", table_option="dbtable"))
register_source_resolver("bigquery", SparkFormatConnector("bigquery", table_option="table"))
register_source_resolver("rest_api", RestApiConnector())
