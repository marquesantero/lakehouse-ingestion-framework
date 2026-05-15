"""Watermarks tipados (simples e compostos)."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ._spark import spark
from ._sql import validate_cols
from .schema import table_exists


def _data_type_map(df: DataFrame) -> Dict[str, str]:
    """Mapa coluna -> tipo simples (ex.: ``timestamp``, ``bigint``)."""
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}


def encode_watermark(df: DataFrame, values: Dict[str, Any]) -> str:
    """Serializa um watermark como JSON tipado e estável.

    Cada coluna vira ``{"type": <simpleString>, "value": <str|None>}``. O tipo
    vem de ``df.schema``, viabilizando reconstrução do literal correto na
    leitura. ``sort_keys=True`` para diffs determinísticos.
    """
    type_map = _data_type_map(df)
    payload = {
        col: {
            "type": type_map.get(col, "string"),
            "value": None if values.get(col) is None else str(values.get(col)),
        }
        for col in values
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def decode_watermark(
    raw: Optional[str], cols: List[str]
) -> Optional[Dict[str, Dict[str, Optional[str]]]]:
    """Decodifica o JSON do watermark e valida a presença das colunas.

    Retorna ``None`` se ``raw`` for vazio. Erra se faltar coluna ou se o JSON
    estiver mal formado.

    Raises:
        ValueError: em payload inválido ou colunas ausentes.
    """
    if not raw:
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"Watermark inválido: {raw}")
    missing = [c for c in cols if c not in parsed]
    if missing:
        raise ValueError(f"Watermark não contém as colunas esperadas: {missing}")
    decoded: Dict[str, Dict[str, Optional[str]]] = {}
    for c in cols:
        item = parsed[c]
        if not isinstance(item, dict) or "value" not in item:
            raise ValueError(f"Watermark inválido para coluna {c}: {item}")
        decoded[c] = {"type": item.get("type") or "string", "value": item.get("value")}
    return decoded


def _watermark_literal(df: DataFrame, col_name: str, values: Dict[str, Dict[str, Optional[str]]]):
    """Reconstrói o literal Spark com o tipo da coluna no DataFrame atual."""
    type_map = _data_type_map(df)
    dtype = values[col_name].get("type") or type_map.get(col_name, "string")
    return F.lit(values[col_name].get("value")).cast(dtype)


def get_watermark(state_table: str, target_table: str, cols: List[str]) -> Optional[str]:
    """Recupera o último watermark conhecido com fallback em cascata.

    Ordem de tentativas:
        1. ``ctrl_ingestion_state.watermark_value``.
        2. ``MAX(col)`` direto da tabela de destino (resiliente a perda do state).
        3. ``None`` (primeira execução ou tabela vazia).
    """
    if not cols:
        return None
    try:
        row = (
            spark.read.table(state_table)
            .where(F.col("target_table") == target_table)
            .select("watermark_value")
            .first()
        )
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass

    if not table_exists(target_table):
        return None

    df = spark.read.table(target_table)
    validate_cols(df, cols, "watermark_columns")
    if len(cols) == 1:
        row = df.agg(F.max(F.col(cols[0])).alias(cols[0])).first()
        if row is None or row[0] is None:
            return None
        return encode_watermark(df, {cols[0]: row[0]})

    row = df.agg(F.max(F.struct(*[F.col(c) for c in cols])).alias("wm")).first()
    if row is None or row[0] is None:
        return None
    return encode_watermark(df, {c: row[0][c] for c in cols})


def apply_watermark(df: DataFrame, cols: List[str], last: Optional[str]) -> DataFrame:
    """Filtra o DataFrame mantendo apenas linhas posteriores ao watermark.

    Para watermark composto, usa comparação lexicográfica:
    ``(c1 > L1) OR (c1 == L1 AND c2 > L2) OR ...``. Se ``cols`` ou ``last`` for
    vazio, retorna o DataFrame original sem filtro.
    """
    if not cols or not last:
        return df
    validate_cols(df, cols, "watermark_columns")
    values = decode_watermark(last, cols)
    if not values:
        return df
    if len(cols) == 1:
        return df.where(F.col(cols[0]) > _watermark_literal(df, cols[0], values))

    expr = F.lit(False)
    for i, c in enumerate(cols):
        eq_previous = F.lit(True)
        for j in range(i):
            prev_col = cols[j]
            eq_previous = eq_previous & (F.col(prev_col) == _watermark_literal(df, prev_col, values))
        expr = expr | (eq_previous & (F.col(c) > _watermark_literal(df, c, values)))
    return df.where(expr)


def compute_watermark(df: DataFrame, cols: List[str]) -> Optional[str]:
    """Calcula o novo watermark a partir do DataFrame.

    Para watermark simples, usa ``MAX(col)``. Para composto, usa
    ``MAX(STRUCT(c1, c2, ...))`` que o Spark compara lexicograficamente.
    Retorna ``None`` se o DataFrame estiver vazio ou todas as linhas forem NULL.
    """
    if not cols:
        return None
    validate_cols(df, cols, "watermark_columns")
    if len(cols) == 1:
        row = df.agg(F.max(F.col(cols[0])).alias(cols[0])).first()
        if row is None or row[0] is None:
            return None
        return encode_watermark(df, {cols[0]: row[0]})
    row = df.agg(F.max(F.struct(*[F.col(c) for c in cols])).alias("wm")).first()
    if row is None or row[0] is None:
        return None
    return encode_watermark(df, {c: row[0][c] for c in cols})
