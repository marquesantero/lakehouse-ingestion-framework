"""Helpers de manipulação de SQL e identificadores.

Funções puras, sem dependência de SparkSession. Centralizam a higiene de
identificadores (escape de crases) e de literais (escape de aspas) para evitar
SQL injection ao montar comandos via f-strings.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional, Union

from pyspark.sql import DataFrame

from .config import CONFIG


def q(identifier: str) -> str:
    """Quota um identificador SQL escapando crases internas.

    Exemplo: ``users`` -> ``\`users\``` ; ``ab\`c`` -> ``\`ab\`\`c\```.
    """
    return f"`{identifier.replace('`', '``')}`"


def qt(table_name: str) -> str:
    """Quota um nome qualificado preservando os pontos.

    ``cat.sch.tbl`` -> ``\`cat\`.\`sch\`.\`tbl\```.
    """
    return ".".join(q(part) for part in table_name.split("."))


def full_table_name(catalog: str, schema: str, table: str) -> str:
    """Concatena catálogo, schema e tabela em um nome completo (sem quoting)."""
    return f"{catalog}.{schema}.{table}"


def utc_now_ts() -> datetime:
    """Timestamp atual em UTC, com timezone explícito."""
    return datetime.now(timezone.utc)


def utc_now_str() -> str:
    """Timestamp UTC atual formatado como ``YYYY-MM-DD HH:MM:SS``."""
    return utc_now_ts().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    """Data UTC atual formatada como ``YYYY-MM-DD``."""
    return utc_now_ts().strftime("%Y-%m-%d")


def new_run_id() -> str:
    """Gera um novo ``run_id`` (UUID4 string)."""
    return str(uuid.uuid4())


def safe_truncate(text: Optional[str], max_len: int = CONFIG.max_error_len) -> Optional[str]:
    """Trunca texto em ``max_len`` caracteres e adiciona marcador.

    Útil para ``error_message`` em ctrl tables — evita estourar STRING grande.
    Retorna ``None`` se o input for ``None``.
    """
    if text is None:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n...TRUNCATED..."


def sql_lit(value: Any) -> str:
    """Converte um valor Python em literal SQL seguro.

    - ``None`` -> ``NULL``
    - ``bool`` -> ``true``/``false``
    - outros -> ``'...'`` com aspas simples internas duplicadas (escape SQL).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: Optional[int]) -> str:
    """Literal SQL inteiro: ``None`` vira ``NULL``, demais são castados via ``int``."""
    return "NULL" if value is None else str(int(value))


def to_json(value: Any) -> str:
    """Serializa um valor para JSON tolerante a tipos exóticos.

    Usa ``default=str`` para datetimes/Decimal etc. Em última instância serializa
    ``str(value)`` para nunca quebrar o caller.
    """
    try:
        return json.dumps(value, default=str, ensure_ascii=False)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


def as_list(value: Optional[Union[str, Iterable[str]]], sep: str = "|") -> List[str]:
    """Normaliza a entrada para uma lista de strings sem espaços e sem vazios.

    Aceita ``None`` (-> ``[]``), string (split por ``sep``) ou qualquer iterável.
    Conveniência para parâmetros vindos de notebooks Databricks (que chegam
    como string única separada por ``|``).
    """
    if not value:
        return []
    if isinstance(value, str):
        return [x.strip() for x in value.split(sep) if x.strip()]
    return [str(x).strip() for x in value if str(x).strip()]


def validate_cols(df: DataFrame, cols: List[str], context: str = "columns") -> None:
    """Verifica que todas as colunas estão presentes no DataFrame.

    Args:
        df: DataFrame a inspecionar.
        cols: Colunas requeridas.
        context: Rótulo usado na mensagem de erro (ex.: ``"merge_keys"``).

    Raises:
        ValueError: se alguma coluna faltar.
    """
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{context} não encontradas: {missing}")
