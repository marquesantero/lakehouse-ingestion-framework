"""Resolução da SparkSession ativa.

Fornece um proxy `spark` lazy que delega para a sessão ativa em runtime,
suportando tanto Databricks (`databricks.sdk.runtime.spark`) quanto qualquer
ambiente PySpark via `SparkSession.getActiveSession()`. Falha com mensagem
clara quando não há sessão.
"""
from __future__ import annotations

import platform
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

_IS_SERVERLESS: Optional[bool] = None


def get_spark() -> SparkSession:
    """Resolve a SparkSession ativa, com fallback explícito.

    Tenta, em ordem: ``databricks.sdk.runtime.spark`` (DBR),
    ``SparkSession.getActiveSession()`` (PySpark padrão) e
    ``SparkSession._instantiatedSession`` (fallback).

    Raises:
        RuntimeError: se nenhuma SparkSession estiver ativa.
    """
    try:
        from databricks.sdk.runtime import spark as dbx_spark  # type: ignore

        if dbx_spark is not None:
            return dbx_spark  # type: ignore[return-value]
    except Exception:
        pass
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession._instantiatedSession  # type: ignore[attr-defined]
    if session is None:
        raise RuntimeError(
            "Nenhuma SparkSession ativa encontrada. "
            "Inicialize uma sessão (ex.: SparkSession.builder.getOrCreate()) "
            "ou execute dentro de um runtime Databricks antes de chamar o framework."
        )
    return session


class _SparkProxy:
    """Proxy módulo-level: cada acesso resolve a sessão ativa no momento da chamada."""

    def __getattr__(self, name: str) -> Any:
        return getattr(get_spark(), name)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "<contractforge._spark.spark proxy>"


spark = _SparkProxy()


def detect_serverless() -> bool:
    """Indica se a sessão está em um cluster Databricks Serverless.

    Lê configurações do Spark e cacheia o resultado a nível de módulo. Em
    caso de erro de leitura, retorna ``False`` (assume cluster tradicional).
    """
    global _IS_SERVERLESS
    if _IS_SERVERLESS is not None:
        return _IS_SERVERLESS
    try:
        conf = get_spark().conf
        checks = [
            conf.get("spark.databricks.serverless.enabled", "false").lower() == "true",
            "serverless" in conf.get("spark.databricks.clusterUsageTags.clusterType", "").lower(),
            "serverless" in conf.get("spark.databricks.clusterUsageTags.clusterName", "").lower(),
        ]
        _IS_SERVERLESS = any(checks)
    except Exception:
        _IS_SERVERLESS = False
    return _IS_SERVERLESS


def runtime_info() -> dict[str, Optional[str]]:
    """Retorna metadados leves do runtime para auditoria operacional."""
    try:
        spark_version = getattr(get_spark(), "version", None)
    except Exception:
        spark_version = None
    return {
        "runtime_type": "serverless" if detect_serverless() else "classic",
        "spark_version": spark_version,
        "python_version": platform.python_version(),
    }


def safe_cache(df: DataFrame, enabled: bool = True) -> DataFrame:
    """Cacheia o DataFrame, degradando silenciosamente em serverless.

    Retorna o DataFrame original (sem cache) se ``enabled=False``, se a sessão
    for serverless, ou se Spark erguer ``NOT_SUPPORTED``/``SERVERLESS``. Outros
    erros são propagados.
    """
    if not enabled or detect_serverless():
        return df
    try:
        return df.cache()
    except Exception as exc:
        if "NOT_SUPPORTED" in str(exc).upper() or "SERVERLESS" in str(exc).upper():
            return df
        raise


def safe_unpersist(df: DataFrame, enabled: bool = True) -> None:
    """Libera o cache do DataFrame com a mesma tolerância do ``safe_cache``."""
    if not enabled or detect_serverless():
        return
    try:
        df.unpersist()
    except Exception as exc:
        if "NOT_SUPPORTED" in str(exc).upper() or "SERVERLESS" in str(exc).upper():
            return
        raise
