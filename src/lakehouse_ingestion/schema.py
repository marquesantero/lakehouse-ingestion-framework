"""Hash, deduplicação, encoding e validação de schema."""
from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import CONTROL_COLUMNS, SchemaPolicy
from ._spark import spark
from ._sql import q, qt, validate_cols


def hash_columns(df: DataFrame, exclude_cols: Optional[List[str]] = None) -> List[str]:
    """Devolve as colunas elegiveis para hash, em ordem alfabetica.

    Exclui sempre as ``CONTROL_COLUMNS`` e qualquer coluna em ``exclude_cols``.
    A ordenacao garante hash estavel se as colunas forem reordenadas.
    """
    exclude = set(CONTROL_COLUMNS)
    if exclude_cols:
        exclude.update(exclude_cols)
    return sorted([c for c in df.columns if c not in exclude])


def hash_from_cols(cols: List[str]):
    """Constroi expressao Spark que produz SHA-256 das colunas.

    Usa Unit Separator (U+001F) como delimitador e NUL (U+0000) como sentinela
    de NULL para evitar colisoes entre, por exemplo, ("a|b","c") e ("a","b|c").
    Resultado e binario de 32 bytes.
    """
    if not cols:
        return F.unhex(F.sha2(F.lit(""), 256))
    return F.unhex(
        F.sha2(
            F.concat_ws("\u001f", *[F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in cols]),
            256,
        )
    )


def add_row_hash(df: DataFrame, exclude_cols: Optional[List[str]] = None) -> DataFrame:
    """Adiciona a coluna ``row_hash`` calculada via ``hash_from_cols``."""
    return df.withColumn("row_hash", hash_from_cols(hash_columns(df, exclude_cols)))


def deduplicate_by_order(df: DataFrame, keys: List[str], order_expr: str) -> DataFrame:
    """Mantem apenas a primeira linha por ``keys`` na ordem de ``order_expr``.

    ``order_expr`` aceita multiplas colunas separadas por virgula com qualquer
    sintaxe SQL valida em ``ORDER BY`` (ex.: "updated_at DESC NULLS LAST, version DESC").
    Se ``keys`` for vazio, retorna o DataFrame inalterado.

    Raises:
        ValueError: se ``order_expr`` nao produzir nenhuma clausula valida.
    """
    if not keys:
        return df
    validate_cols(df, keys, "dedup keys")
    order_parts = [part.strip() for part in order_expr.split(",") if part.strip()]
    if not order_parts:
        raise ValueError("dedup_order_expr informado, mas nenhuma ordenação válida foi encontrada")

    source_view = f"__ingest_dedup_src_{uuid.uuid4().hex}"
    rn_col = f"__ingest_dedup_rn_{uuid.uuid4().hex}"
    select_cols = ", ".join(q(c) for c in df.columns)
    partition_clause = ", ".join(q(k) for k in keys)
    order_clause = ", ".join(order_parts)
    df.createOrReplaceTempView(source_view)
    # O DataFrame retornado é lazy; em Spark Connect a view pode ser resolvida
    # apenas na ação posterior. Por isso não removemos a temp view aqui.
    return spark.sql(f"""
        SELECT {select_cols}
        FROM (
            SELECT
                {select_cols},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_clause}
                    ORDER BY {order_clause}
                ) AS {q(rn_col)}
            FROM {q(source_view)}
        ) __dedup
        WHERE {q(rn_col)} = 1
    """)


def build_custom_keys(df: DataFrame, custom_keys: Dict[str, List[str]]) -> DataFrame:
    """Adiciona colunas-chave compostas concatenando outras com separador ``|``.

    Util quando uma chave logica e composta mas a tabela quer uma unica coluna
    para indexacao ou ``merge_keys`` simples. Valores NULL viram string vazia.
    """
    for key_name, cols in custom_keys.items():
        validate_cols(df, cols, f"custom_keys.{key_name}")
        df = df.withColumn(
            key_name,
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
        )
    return df


def fix_encoding(df: DataFrame, enabled: bool, encoding: str, columns: List[str]) -> DataFrame:
    """Re-decodifica colunas string com ``encoding`` (ex.: Windows-1252).

    Se ``columns`` for vazio, atua em todas as colunas string.
    ``enabled=False`` e passthrough. Use so em emergencia - a solucao correta
    e ler com o charset certo na origem.
    """
    if not enabled:
        return df
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    cols_to_fix = columns or string_cols
    for col_name in cols_to_fix:
        if col_name in string_cols:
            df = df.withColumn(col_name, F.decode(F.col(col_name).cast("binary"), encoding))
    return df


def schema_signature(df: DataFrame) -> str:
    """Serializa o schema do DataFrame como JSON (nome, tipo, nullable)."""
    return json.dumps(
        [(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields],
        ensure_ascii=False,
    )


def table_exists(full_name: str) -> bool:
    """Verifica se uma tabela existe usando nomes de 1, 2 ou 3 partes."""
    try:
        if spark.catalog.tableExists(full_name):
            return True
    except Exception:
        pass
    try:
        spark.sql(f"DESCRIBE TABLE {qt(full_name)}")
        return True
    except Exception:
        return False


_INTEGER_ORDER = {"tinyint": 0, "smallint": 1, "int": 2, "bigint": 3}
_FLOAT_ORDER = {"float": 0, "double": 1}


def _decimal_parts(dtype: str) -> Optional[tuple[int, int]]:
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", dtype)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def is_type_widening(source_type: str, target_type: str) -> bool:
    """Retorna se ``target_type`` pode ser alargado para ``source_type`` sem perda esperada."""
    if source_type == target_type:
        return True
    if source_type in _INTEGER_ORDER and target_type in _INTEGER_ORDER:
        return _INTEGER_ORDER[source_type] >= _INTEGER_ORDER[target_type]
    if source_type in _FLOAT_ORDER and target_type in _FLOAT_ORDER:
        return _FLOAT_ORDER[source_type] >= _FLOAT_ORDER[target_type]
    if source_type == "double" and target_type in _INTEGER_ORDER:
        return True
    if source_type == "timestamp" and target_type == "date":
        return True
    source_decimal = _decimal_parts(source_type)
    target_decimal = _decimal_parts(target_type)
    if source_decimal and target_decimal:
        source_precision, source_scale = source_decimal
        target_precision, target_scale = target_decimal
        return source_precision >= target_precision and source_scale >= target_scale
    return False


def validate_schema_policy(
    df: DataFrame,
    target: str,
    policy: SchemaPolicy,
    allow_type_widening: bool = False,
) -> Dict[str, Any]:
    """Compara schema do DataFrame com o do target e aplica a politica.

    Retorna um dict com ``status``, ``added_columns``, ``removed_columns`` e
    ``type_changes``. Para tabela inexistente devolve ``status="new_table"``.

    Raises:
        ValueError: se ``policy="strict"`` e houver divergencia, ou se
            ``policy="additive_only"`` e houver remocoes/mudancas de tipo.
    """
    if not table_exists(target):
        return {"status": "new_table", "added_columns": [], "removed_columns": [], "type_changes": []}
    target_df = spark.read.table(target)
    src = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    tgt = {f.name: f.dataType.simpleString() for f in target_df.schema.fields}
    added = sorted([c for c in src if c not in tgt])
    removed = sorted([c for c in tgt if c not in src and c not in CONTROL_COLUMNS])
    type_changes = []
    for c in sorted(src.keys() & tgt.keys()):
        if src[c] == tgt[c]:
            continue
        widening_allowed = allow_type_widening and is_type_widening(src[c], tgt[c])
        type_changes.append(
            {
                "column": c,
                "source": src[c],
                "target": tgt[c],
                "allowed": widening_allowed,
                "change": "type_widening" if widening_allowed else "type_change",
            }
        )
    blocking_type_changes = [change for change in type_changes if not change.get("allowed")]

    if policy == "strict" and (added or removed or type_changes):
        raise ValueError(
            f"Schema policy strict violada: added={added}, removed={removed}, type_changes={type_changes}"
        )
    if policy == "additive_only" and (removed or blocking_type_changes):
        raise ValueError(
            f"Schema policy additive_only violada: removed={removed}, type_changes={blocking_type_changes}"
        )
    if policy == "permissive" and blocking_type_changes:
        raise ValueError(
            "Schema policy permissive não aplica mudanças de tipo potencialmente destrutivas. "
            f"Use allow_type_widening=True apenas para alargamentos seguros. type_changes={blocking_type_changes}"
        )
    return {
        "status": "checked",
        "added_columns": added,
        "removed_columns": removed,
        "type_changes": type_changes,
        "allow_type_widening": allow_type_widening,
    }


def sync_delta_schema(
    df: DataFrame,
    target: str,
    schema_changes: Dict[str, Any],
    policy: SchemaPolicy,
) -> None:
    """Aplica evolução aditiva de schema, se a política permitir.

    Adiciona colunas novas e aplica alargamento de tipo quando
    ``allow_type_widening`` foi validado previamente. Nunca remove colunas.
    """
    if not table_exists(target):
        return
    if policy not in {"permissive", "additive_only"}:
        return
    added = schema_changes.get("added_columns") or []
    fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    cols_sql = ", ".join(f"{q(c)} {fields[c]}" for c in added if c in fields)
    if cols_sql:
        spark.sql(f"ALTER TABLE {qt(target)} ADD COLUMNS ({cols_sql})")
    for change in schema_changes.get("type_changes") or []:
        if not change.get("allowed"):
            continue
        column = change["column"]
        source_type = change["source"]
        spark.sql(f"ALTER TABLE {qt(target)} ALTER COLUMN {q(column)} TYPE {source_type}")
        change["applied"] = True
