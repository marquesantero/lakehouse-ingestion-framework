"""Hash, deduplicação, encoding e validação de schema."""
from __future__ import annotations

import json
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
    """Verifica se ``catalog.schema.table`` existe.

    Usa ``spark.catalog.tableExists`` quando possivel; cai para
    ``DESCRIBE TABLE`` se o nome nao puder ser dividido em tres partes.
    """
    try:
        catalog, schema, table = full_name.split(".", 2)
        return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")
    except Exception:
        try:
            spark.sql(f"DESCRIBE TABLE {qt(full_name)}")
            return True
        except Exception:
            return False


def validate_schema_policy(df: DataFrame, target: str, policy: SchemaPolicy) -> Dict[str, Any]:
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
    type_changes = sorted(
        [
            {"column": c, "source": src[c], "target": tgt[c]}
            for c in src.keys() & tgt.keys()
            if src[c] != tgt[c]
        ],
        key=lambda x: x["column"],
    )

    if policy == "strict" and (added or removed or type_changes):
        raise ValueError(
            f"Schema policy strict violada: added={added}, removed={removed}, type_changes={type_changes}"
        )
    if policy == "additive_only" and (removed or type_changes):
        raise ValueError(
            f"Schema policy additive_only violada: removed={removed}, type_changes={type_changes}"
        )
    return {
        "status": "checked",
        "added_columns": added,
        "removed_columns": removed,
        "type_changes": type_changes,
    }


def sync_delta_schema(
    df: DataFrame,
    target: str,
    schema_changes: Dict[str, Any],
    policy: SchemaPolicy,
) -> None:
    """Aplica ``ALTER TABLE ADD COLUMNS`` para colunas novas, se a politica permitir.

    No-op se a tabela nao existe, se nao ha colunas adicionadas, ou se a
    politica e ``strict``. Tipos vem do DataFrame fonte.
    """
    if not table_exists(target):
        return
    added = schema_changes.get("added_columns") or []
    if not added:
        return
    if policy not in {"permissive", "additive_only"}:
        return
    fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    cols_sql = ", ".join(f"{q(c)} {fields[c]}" for c in added if c in fields)
    if cols_sql:
        spark.sql(f"ALTER TABLE {qt(target)} ADD COLUMNS ({cols_sql})")
