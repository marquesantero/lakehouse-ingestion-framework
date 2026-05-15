"""Motores de escrita por modo (scd0_append, scd0_overwrite, scd1_upsert, scd1_hash_diff, scd2_historical, snapshot_soft_delete"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import CONFIG, VALID_WRITE_MODES, MergeStrategy, WriteMode
from .plan import IngestionPlan
from .schema import (
    add_row_hash,
    deduplicate_by_order,
    hash_columns,
    hash_from_cols,
    table_exists,
)
from ._spark import spark
from ._sql import q, qt, sql_lit, validate_cols

logger = logging.getLogger("contractforge")

WriteHandler = Callable[[IngestionPlan, DataFrame, str, int], int]


def ensure_delta_table(
    df: DataFrame,
    target: str,
    cluster_cols: List[str],
    partition_col: Optional[str],
    delta_properties: Optional[Dict[str, str]] = None,
) -> bool:
    """Cria a tabela Delta com schema vazio se ainda não existe.

    Usa SQL ``CREATE TABLE`` com schema explícito para evitar diferenças de
    comportamento do ``saveAsTable`` entre Spark local, Delta V2 e Databricks.
    Aplica particionamento na criação e ``CLUSTER BY`` depois, quando suportado.
    Retorna ``True`` se criou, ``False`` se já existia.
    """
    if table_exists(target):
        return False
    cols_sql = ", ".join(f"{q(field.name)} {field.dataType.simpleString()}" for field in df.schema.fields)
    partition_sql = ""
    if partition_col and not cluster_cols:
        validate_cols(df, [partition_col], "partition_column")
        partition_sql = f" PARTITIONED BY ({q(partition_col)})"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {qt(target)} ({cols_sql}) USING DELTA{partition_sql}")
    if cluster_cols:
        validate_cols(df, cluster_cols, "cluster_columns")
        spark.sql(f"ALTER TABLE {qt(target)} CLUSTER BY ({', '.join(q(c) for c in cluster_cols)})")
    apply_delta_properties(target, delta_properties)
    return True


def apply_delta_properties(target: str, delta_properties: Optional[Dict[str, str]]) -> None:
    """Aplica TBLPROPERTIES Delta na criação da tabela."""
    if not delta_properties:
        return
    properties_sql = ", ".join(
        f"{sql_lit(key)} = {sql_lit(value)}"
        for key, value in sorted(delta_properties.items())
    )
    spark.sql(f"ALTER TABLE {qt(target)} SET TBLPROPERTIES ({properties_sql})")


def run_optimize(target: str, zorder_cols: List[str]) -> None:
    """Executa ``OPTIMIZE``, com ZORDER opcional pelas colunas listadas."""
    if zorder_cols:
        spark.sql(f"OPTIMIZE {qt(target)} ZORDER BY ({', '.join(q(c) for c in zorder_cols)})")
    else:
        spark.sql(f"OPTIMIZE {qt(target)}")


def delta_version(target: str) -> Optional[int]:
    """Versão Delta atual da tabela, ou ``None`` se não existir/erro."""
    try:
        row = spark.sql(f"DESCRIBE HISTORY {qt(target)} LIMIT 1").select("version").first()
        return None if row is None else int(row[0])
    except Exception:
        return None


def latest_operation_metrics(target: str) -> Dict[str, Any]:
    """Métricas da última operação Delta (``version``, ``operation``, ``operationMetrics``).

    Devolve ``{}`` se a tabela não existe ou a leitura falhar.
    """
    try:
        row = spark.sql(f"DESCRIBE HISTORY {qt(target)} LIMIT 1").first()
        if row is None:
            return {}
        d = row.asDict(recursive=True)
        return {
            "version": d.get("version"),
            "operation": d.get("operation"),
            "operationMetrics": d.get("operationMetrics"),
        }
    except Exception:
        return {}


def extract_row_metrics(metrics: Dict[str, Any]) -> Dict[str, int]:
    """Mapeia operationMetrics do Delta para contadores normalizados.

    Para MERGE, Delta retorna numTargetRows{Inserted,Updated,Deleted}; para APPEND/WRITE
    apenas numOutputRows. Quando só temos numOutputRows (modos append puros e
    scd1_hash_diff) tratamos como inserts.
    """
    op = metrics.get("operationMetrics") or {}

    def parse(*names: str) -> int:
        for n in names:
            if n in op and op[n] is not None:
                try:
                    return int(op[n])
                except Exception:
                    return 0
        return 0

    return {
        "rows_inserted": parse("numTargetRowsInserted", "numOutputRows"),
        "rows_updated": parse("numTargetRowsUpdated"),
        "rows_deleted": parse("numTargetRowsDeleted"),
    }


def logical_row_metrics(plan: IngestionPlan, rows_written: int) -> Dict[str, int]:
    """Contadores lógicos calculados pela lib quando Delta history é insuficiente."""
    metrics = {
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
        "rows_affected": int(rows_written or 0),
    }
    if rows_written <= 0:
        return metrics
    if plan.mode in {"scd0_append", "scd0_overwrite", "scd1_hash_diff", "scd2_historical"}:
        metrics["rows_inserted"] = rows_written
    elif plan.mode == "scd1_upsert" and plan.merge_strategy == "replace_partitions":
        metrics["rows_inserted"] = rows_written
    return metrics


def resolve_write_metrics(
    plan: IngestionPlan,
    rows_written: int,
    delta_metrics: Dict[str, Any],
) -> tuple[Dict[str, int], Dict[str, Any], str]:
    """Combina métricas lógicas da lib com evidência do Delta history.

    Retorna ``(row_metrics, operation_metrics, metrics_source)``. O campo
    ``operation_metrics.logicalMetrics`` sempre existe para manter rastreio
    consistente mesmo quando ``DESCRIBE HISTORY`` varia por runtime.
    """
    logical = logical_row_metrics(plan, rows_written)
    operation_metrics = dict(delta_metrics or {})
    operation_metrics["logicalMetrics"] = logical
    if operation_metrics.get("operationMetrics"):
        row_metrics = extract_row_metrics(operation_metrics)
        row_metrics["rows_affected"] = logical["rows_affected"]
        return row_metrics, operation_metrics, "mixed"
    return logical, operation_metrics, "logical"


def affected_partition_values(df: DataFrame, partition_col: Optional[str]) -> List[Any]:
    """Coleta valores distintos da coluna de partição até o limite configurado.

    Usado em SCD1 hash diff (pré-filtro do target) e em ``replace_partitions``.
    Loga warning se atingir ``CONFIG.max_partition_predicate_values``.
    """
    if not partition_col or partition_col not in df.columns:
        return []
    values = [
        r[0]
        for r in df.select(partition_col)
        .distinct()
        .limit(CONFIG.max_partition_predicate_values)
        .collect()
    ]
    if len(values) == CONFIG.max_partition_predicate_values:
        logger.warning(
            f"Leitura de valores distintos de {partition_col} atingiu o limite configurado; "
            "a lista retornada pode estar truncada."
        )
    return values


def write_strategy(mode: WriteMode) -> str:
    """Mapa do modo lógico para o rótulo de estratégia (apenas para logs)."""
    return {
        "scd0_append": "APPEND",
        "scd0_overwrite": "OVERWRITE",
        "scd1_upsert": "MERGE",
        "scd1_hash_diff": "HASH_DIFF_APPEND",
        "scd2_historical": "SCD2_MERGE",
        "snapshot_soft_delete": "SNAPSHOT_MERGE",
    }.get(mode, f"CUSTOM:{mode}")


def write_append(
    df: DataFrame,
    target: str,
    cluster_cols: List[str],
    partition_col: Optional[str],
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``scd0_append``: APPEND simples com ``mergeSchema=true``.

    Cria a tabela se não existe. Retorna número de linhas escritas.
    """
    ensure_delta_table(df, target, cluster_cols, partition_col, delta_properties)
    count = expected_count if expected_count is not None else df.count()
    if count:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    return count


def write_overwrite(
    df: DataFrame,
    target: str,
    partition_col: Optional[str],
    partition_value: Optional[str],
    cluster_cols: List[str],
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``scd0_overwrite``: OVERWRITE total ou por partição.

    Quando ``partition_col`` e ``partition_value`` são informados (e não há
    cluster), usa ``replaceWhere`` para sobrescrever só a partição alvo.
    """
    ensure_delta_table(df, target, cluster_cols, partition_col, delta_properties)
    count = expected_count if expected_count is not None else df.count()
    writer = df.write.format("delta").mode("overwrite")
    if partition_col and partition_value and not cluster_cols:
        writer = writer.option(
            "replaceWhere",
            f"{q(partition_col)} = '{str(partition_value).replace(chr(39), chr(39) + chr(39))}'",
        ).option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")
    if partition_col and not cluster_cols:
        writer = writer.partitionBy(partition_col)
    try:
        writer.saveAsTable(target)
    except Exception as exc:
        if "does not support truncate" not in str(exc):
            raise
        logger.warning(
            "Runtime Delta não suporta truncate via overwrite para %s; usando fallback SQL compatível.",
            target,
        )
        if partition_col and partition_value and not cluster_cols:
            escaped_value = str(partition_value).replace("'", "''")
            spark.sql(f"DELETE FROM {qt(target)} WHERE {q(partition_col)} = '{escaped_value}'")
            if count:
                df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
        else:
            spark.sql(f"DROP TABLE IF EXISTS {qt(target)}")
            ensure_delta_table(df, target, cluster_cols, partition_col, delta_properties)
            if count:
                df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    return count


def write_upsert(
    df: DataFrame,
    target: str,
    keys: List[str],
    partition_col: Optional[str],
    partition_values: Optional[List[Any]],
    strategy: MergeStrategy,
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``scd1_upsert``: estado atual via MERGE.

    Três estratégias:

    - ``delta`` (default): MERGE puro com ``t.k <=> s.k``.
    - ``delta_by_partition``: MERGE com predicado adicional ``IN (vals)``,
      reduzindo arquivos varridos.
    - ``replace_partitions``: OVERWRITE com ``replaceWhere``, mais rápido
      quando o source contém o estado completo das partições afetadas.

    Raises:
        ValueError: se ``strategy="replace_partitions"`` sem
            ``merge_partition_column`` definido.
    """
    validate_cols(df, keys, "merge_keys")
    ensure_delta_table(df, target, [], partition_col, delta_properties)
    count = expected_count if expected_count is not None else df.count()
    if count == 0:
        return 0

    if strategy == "replace_partitions":
        if not partition_col or not partition_values:
            raise ValueError("replace_partitions requer merge_partition_column com valores detectados")
        vals = ", ".join(sql_lit(v) for v in partition_values)
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
            "replaceWhere", f"{q(partition_col)} IN ({vals})"
        ).saveAsTable(target)
        return count

    source_view = f"__ingest_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(source_view)
    key_cond = " AND ".join([f"t.{q(k)} <=> s.{q(k)}" for k in keys])
    if strategy == "delta_by_partition" and partition_col and partition_values:
        vals = ", ".join(sql_lit(v) for v in partition_values)
        key_cond += f" AND t.{q(partition_col)} IN ({vals})"
    update_cols = [c for c in df.columns if c not in keys]
    update_set = ", ".join([f"t.{q(c)} = s.{q(c)}" for c in update_cols])
    insert_cols = ", ".join(q(c) for c in df.columns)
    insert_vals = ", ".join(f"s.{q(c)}" for c in df.columns)
    try:
        spark.sql(f"""
            MERGE INTO {qt(target)} t
            USING {q(source_view)} s
            ON {key_cond}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
    finally:
        spark.catalog.dropTempView(source_view)
    return count


def write_scd1_hash_diff(
    df: DataFrame,
    target: str,
    hash_keys: List[str],
    hash_exclude: List[str],
    cluster_cols: List[str],
    partition_col: Optional[str],
    latest_order_expr: Optional[str],
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``scd1_hash_diff``: APPEND apenas de linhas novas ou alteradas.

    Calcula ``row_hash`` da source, deduplica o "atual" do target via
    ``latest_order_expr`` quando informado. Sem expressão explícita, usa
    ``ingestion_sequence`` ou ``ingestion_ts_utc`` quando disponíveis. O fallback
    antigo em ``ingestion_date`` só é aceito se não houver múltiplas versões por
    chave no target, porque a data não ordena execuções no mesmo dia.
    """
    validate_cols(df, hash_keys, "hash_keys")
    df_hashed = add_row_hash(df, hash_exclude)
    ensure_delta_table(df_hashed, target, cluster_cols, partition_col, delta_properties)

    if not table_exists(target) or spark.read.table(target).limit(1).count() == 0:
        count = expected_count if expected_count is not None else df_hashed.count()
        if count:
            df_hashed.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(target)
        return count

    target_df = spark.read.table(target)
    target_cols = set(target_df.columns)
    if partition_col and partition_col in df_hashed.columns:
        part_values = [
            r[0]
            for r in df_hashed.select(partition_col)
            .distinct()
            .limit(CONFIG.max_partition_predicate_values)
            .collect()
        ]
        if len(part_values) == CONFIG.max_partition_predicate_values:
            logger.warning(
                f"scd1_hash_diff atingiu o limite configurado de valores distintos em "
                f"{partition_col}; o predicado pode estar truncado e a leitura do target "
                "pode ser maior que o necessário."
            )
        if part_values:
            target_df = target_df.where(F.col(partition_col).isin(part_values))

    if latest_order_expr:
        order_expr = latest_order_expr
    elif "ingestion_sequence" in target_cols:
        order_expr = "ingestion_sequence DESC NULLS LAST"
    elif "ingestion_ts_utc" in target_cols:
        order_expr = "ingestion_ts_utc DESC NULLS LAST, __run_id DESC NULLS LAST"
        ambiguous_legacy = (
            target_df.groupBy(*hash_keys)
            .agg(F.count(F.lit(1)).alias("__cnt"), F.max(F.col("ingestion_ts_utc")).alias("__max_ingestion_ts_utc"))
            .where((F.col("__cnt") > 1) & F.col("__max_ingestion_ts_utc").isNull())
            .limit(1)
            .count()
        )
        if ambiguous_legacy:
            raise ValueError(
                "scd1_hash_diff encontrou múltiplas versões por chave com ingestion_ts_utc nulo no target. "
                "Informe dedup_order_expr para migração do histórico legado ou regrave o target com "
                "ingestion_ts_utc/ingestion_sequence."
            )
    else:
        order_expr = None

    if order_expr:
        target_latest = deduplicate_by_order(target_df, hash_keys, order_expr)
    else:
        duplicate_key = (
            target_df.groupBy(*hash_keys)
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_key:
            raise ValueError(
                "scd1_hash_diff encontrou múltiplas versões por chave no target, mas não há ordenação "
                "determinística para escolher o último estado. Informe dedup_order_expr ou regrave o target "
                "com ingestion_ts_utc/ingestion_sequence."
            )
        target_latest = target_df

    target_hash = target_latest.select(*hash_keys, F.col("row_hash").alias("__tgt_row_hash"))
    diff = (
        df_hashed
        .join(target_hash, on=hash_keys, how="left")
        .where(F.col("__tgt_row_hash").isNull() | (F.col("row_hash") != F.col("__tgt_row_hash")))
        .select(*[F.col(c) for c in df_hashed.columns])
    )
    count = diff.count()
    if count:
        diff.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    return count


def write_snapshot_soft_delete(
    df: DataFrame,
    target: str,
    keys: List[str],
    cluster_cols: List[str],
    partition_col: Optional[str],
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``snapshot_soft_delete``: sincronização por snapshot completo.

    Insere novos, atualiza alterados (compara ``row_hash`` ou ``is_active=false``
    para "ressuscitar") e marca ``is_active=false`` + ``deleted_at=now()`` em
    linhas presentes no target mas ausentes na source (``WHEN NOT MATCHED BY
    SOURCE``).

    A source DEVE ser completa — qualquer filtro (incluindo watermark) leva a
    soft-delete incorreto.
    """
    validate_cols(df, keys, "merge_keys")
    df_src = (
        add_row_hash(df)
        .withColumn("is_active", F.lit(True))
        .withColumn("deleted_at", F.lit(None).cast("timestamp"))
    )
    ensure_delta_table(df_src, target, cluster_cols, partition_col, delta_properties)
    count = expected_count if expected_count is not None else df_src.count()
    if count == 0:
        return 0

    cond = " AND ".join([f"t.{q(k)} <=> s.{q(k)}" for k in keys])
    source_view = f"__snapshot_src_{uuid.uuid4().hex}"
    df_src.createOrReplaceTempView(source_view)
    update_set = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in df_src.columns if c not in keys)
    insert_cols_sql = ", ".join(q(c) for c in df_src.columns)
    insert_vals_sql = ", ".join(f"s.{q(c)}" for c in df_src.columns)
    try:
        spark.sql(f"""
            MERGE INTO {qt(target)} t
            USING {q(source_view)} s
            ON {cond}
            WHEN MATCHED AND (NOT (t.row_hash <=> s.row_hash) OR t.is_active = false)
                THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
            WHEN NOT MATCHED BY SOURCE AND t.is_active = true THEN UPDATE SET
                t.is_active = false,
                t.deleted_at = current_timestamp()
        """)
    finally:
        spark.catalog.dropTempView(source_view)
    return count


def _changed_columns_expr(change_cols: List[str]) -> str:
    """Constrói SQL que produz CSV das colunas mudadas em SCD2."""
    parts = [
        f"CASE WHEN NOT (t.{q(c)} <=> s.{q(c)}) THEN '{c}' ELSE NULL END" for c in change_cols
    ]
    return f"concat_ws(',', {', '.join(parts)})"


def write_scd2(
    df: DataFrame,
    target: str,
    keys: List[str],
    change_cols: List[str],
    effective_from_col: Optional[str],
    cluster_cols: List[str],
    delta_properties: Optional[Dict[str, str]] = None,
    expected_count: Optional[int] = None,
) -> int:
    """Modo ``scd2_historical``: histórico completo por versões.

    Hash é calculado SOMENTE sobre ``change_cols`` (mudanças fora dessas colunas
    NÃO geram nova versão). Quando ``change_cols`` é vazio, usa todas as
    colunas exceto chaves e ``CONTROL_COLUMNS``.

    Trick de staging: cada linha changed gera duas variantes — uma com
    ``__merge_key_*`` igual à chave (forçando UPDATE da versão atual) e outra
    com ``__merge_key_*=NULL`` (forçando INSERT da nova versão). Isso permite
    que o MERGE Delta dispare ambas as ações para a mesma chave, viabilizando
    versionamento mesmo em chaves reaparecidas.
    """
    validate_cols(df, keys, "merge_keys")
    if change_cols:
        validate_cols(df, change_cols, "scd2_change_columns")
    else:
        change_cols = [c for c in hash_columns(df, []) if c not in keys]
    if effective_from_col:
        validate_cols(df, [effective_from_col], "scd2_effective_from_column")
        effective_expr = F.col(effective_from_col).cast("timestamp")
    else:
        effective_expr = F.current_timestamp()

    src = (
        df.withColumn("valid_from", effective_expr)
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("row_hash", hash_from_cols(change_cols))
        .withColumn("changed_columns", F.lit(None).cast("string"))
    )
    ensure_delta_table(src, target, cluster_cols, None, delta_properties)
    incoming_count = expected_count if expected_count is not None else src.count()
    if incoming_count == 0:
        return 0

    if spark.read.table(target).limit(1).count() == 0:
        src.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
        return incoming_count

    target_current = (
        spark.read.table(target)
        .where(F.col("is_current") == F.lit(True))
        .select(*keys, F.col("row_hash").alias("__tgt_row_hash"))
    )
    changed = (
        src
        .join(target_current, on=keys, how="left")
        .where(F.col("__tgt_row_hash").isNull() | (F.col("row_hash") != F.col("__tgt_row_hash")))
        .select(*[F.col(c) for c in src.columns], F.col("__tgt_row_hash"))
    )
    insert_count = changed.count()
    if insert_count == 0:
        return 0

    merge_key_cols = [f"__merge_key_{k}" for k in keys]
    insert_stage = changed
    for mk in merge_key_cols:
        insert_stage = insert_stage.withColumn(mk, F.lit(None))

    update_stage = changed.where(F.col("__tgt_row_hash").isNotNull())
    for k, mk in zip(keys, merge_key_cols):
        update_stage = update_stage.withColumn(mk, F.col(k))

    staged = insert_stage.unionByName(update_stage, allowMissingColumns=True).drop(
        "__tgt_row_hash"
    )
    source_view = f"__scd2_stage_{uuid.uuid4().hex}"
    staged.createOrReplaceTempView(source_view)

    key_cond = " AND ".join([f"t.{q(k)} <=> s.{q('__merge_key_' + k)}" for k in keys])
    changed_expr = _changed_columns_expr(change_cols)
    insert_cols = [c for c in src.columns]
    insert_cols_sql = ", ".join(q(c) for c in insert_cols)
    insert_vals_sql = ", ".join(f"s.{q(c)}" for c in insert_cols)
    try:
        spark.sql(f"""
            MERGE INTO {qt(target)} t
            USING {q(source_view)} s
            ON {key_cond} AND t.is_current = true
            WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
                t.valid_to = current_timestamp(),
                t.is_current = false,
                t.changed_columns = {changed_expr}
            WHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
        """)
    finally:
        spark.catalog.dropTempView(source_view)
    return insert_count


def _write_append_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    return write_append(
        df, target, plan.cluster_columns, plan.partition_column, plan.delta_properties, effective_rows
    )


def _write_overwrite_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    return write_overwrite(
        df,
        target,
        plan.partition_column,
        plan.partition_value,
        plan.cluster_columns,
        plan.delta_properties,
        effective_rows,
    )


def _write_upsert_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    merge_partition_col = plan.merge_partition_column or plan.partition_column
    return write_upsert(
        df,
        target,
        plan.merge_keys,
        merge_partition_col,
        affected_partition_values(df, merge_partition_col),
        plan.merge_strategy,
        plan.delta_properties,
        effective_rows,
    )


def _write_hash_diff_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    return write_scd1_hash_diff(
        df,
        target,
        plan.hash_keys,
        plan.hash_exclude_columns,
        plan.cluster_columns,
        plan.partition_column,
        plan.dedup_order_expr,
        plan.delta_properties,
        effective_rows,
    )


def _write_snapshot_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    return write_snapshot_soft_delete(
        df,
        target,
        plan.merge_keys,
        plan.cluster_columns,
        plan.partition_column,
        plan.delta_properties,
        effective_rows,
    )


def _write_scd2_handler(plan: IngestionPlan, df: DataFrame, target: str, effective_rows: int) -> int:
    return write_scd2(
        df,
        target,
        plan.merge_keys,
        plan.scd2_change_columns,
        plan.scd2_effective_from_column,
        plan.cluster_columns,
        plan.delta_properties,
        effective_rows,
    )


WRITE_MODE_REGISTRY: Dict[str, WriteHandler] = {
    "scd0_append": _write_append_handler,
    "scd0_overwrite": _write_overwrite_handler,
    "scd1_upsert": _write_upsert_handler,
    "scd1_hash_diff": _write_hash_diff_handler,
    "snapshot_soft_delete": _write_snapshot_handler,
    "scd2_historical": _write_scd2_handler,
}


def register_write_mode(mode: str, handler: WriteHandler, *, overwrite: bool = False) -> None:
    """Registra um motor de escrita customizado.

    O handler recebe ``(plan, df, target, effective_rows)`` e deve retornar o
    número lógico de linhas escritas/afetadas.
    """
    normalized = str(mode or "").strip()
    if not normalized:
        raise ValueError("mode customizado não pode ser vazio")
    if not callable(handler):
        raise ValueError("handler de write mode deve ser callable")
    if normalized in WRITE_MODE_REGISTRY and not overwrite:
        raise ValueError(f"write mode já registrado: {normalized}")
    WRITE_MODE_REGISTRY[normalized] = handler
    VALID_WRITE_MODES.add(normalized)


def execute_write_mode(
    plan: IngestionPlan,
    df: DataFrame,
    target: str,
    effective_rows: int,
) -> int:
    """Despacha para o motor de escrita correto a partir de ``plan.mode``.

    Curto-circuita em zero linhas (não chama o motor). Calcula
    ``affected_partition_values`` apenas se há ``merge_partition_column``.

    Raises:
        ValueError: se ``plan.mode`` não corresponder a nenhum motor.
    """
    if effective_rows == 0:
        return 0
    handler = WRITE_MODE_REGISTRY.get(plan.mode)
    if handler:
        return handler(plan, df, target, effective_rows)
    raise ValueError(f"Modo não suportado: {plan.mode}")
