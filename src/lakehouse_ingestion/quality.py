"""Quality gates e quarentena.

Implementa avaliação de regras em uma única passagem agregada (sum(when(...))) sempre
que possível, reduzindo o I/O em datasets grandes. unique_key continua como groupBy
separado por exigir contagem por grupo.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import CONFIG, VALID_QUALITY_RULE_SEVERITIES
from .plan import QualityRules
from ._spark import spark
from ._sql import to_json, utc_now_str, validate_cols

#: Regras de qualidade que NÃO são quarentenáveis em nível de linha. Falhas
#: nessas regras descrevem propriedades do conjunto (chave única, presença de
#: colunas, contagem mínima) e não conseguem isolar linhas problemáticas. Quando
#: ``on_quality_fail="quarantine"`` e qualquer dessas regras falhar, o
#: orquestrador escala a ação para ``"fail"``.
ABORT_ONLY_RULES = frozenset({"required_columns", "unique_key", "min_rows"})
QualityRuleEvaluator = Callable[[DataFrame, str, Dict[str, Any]], Dict[str, Any]]
QUALITY_RULE_REGISTRY: Dict[str, QualityRuleEvaluator] = {}


def register_quality_rule(rule_type: str, evaluator: QualityRuleEvaluator, *, overwrite: bool = False) -> None:
    """Registra uma regra de qualidade customizada.

    O evaluator recebe ``(df, rule_name, config)`` e retorna um dict com:
    ``failed_count`` (obrigatório), ``details``/``message`` opcionais e
    ``condition`` opcional para severidade ``quarantine``.
    """
    normalized = str(rule_type or "").strip()
    if not normalized:
        raise ValueError("quality rule type não pode ser vazio")
    if not callable(evaluator):
        raise ValueError("quality rule evaluator deve ser callable")
    if normalized in QUALITY_RULE_REGISTRY and not overwrite:
        raise ValueError(f"quality rule já registrada: {normalized}")
    QUALITY_RULE_REGISTRY[normalized] = evaluator


def is_abort_only_failure(rule_name: str) -> bool:
    """Indica se ``rule_name`` é uma regra abortiva (não quarentenável).

    Os nomes ``not_null:<col>``, ``accepted_values:<col>`` e
    ``max_null_ratio:<col>`` carregam o nome da coluna depois de ``:`` e são
    quarentenáveis. As regras abortivas usam o nome puro.
    """
    base = rule_name.split(":", 1)[0]
    return base in ABORT_ONLY_RULES


def _safe_agg_alias(prefix: str, key: str) -> str:
    """Cria alias estável e seguro para a agregação (evita caracteres problemáticos)."""
    safe = "".join(ch if ch.isalnum() else "_" for ch in key)
    return f"{prefix}__{safe}"


def _row_int(row: Any, field: str) -> int:
    """Lê inteiro de uma Row agregada tratando NULL como zero."""
    if row is None:
        return 0
    return int(row[field] or 0)


def evaluate_quality(
    df: DataFrame,
    rules: Optional[QualityRules],
    run_id: str,
    target: str,
) -> Tuple[str, List[Dict[str, Any]], DataFrame, DataFrame, int]:
    """Avalia regras de qualidade em uma única passagem agregada quando possível.

    Regras de coluna (``not_null``, ``accepted_values``, ``max_null_ratio``)
    são consolidadas em uma única ``df.agg(...)``. Regras estruturais
    (``required_columns``, ``min_rows``, ``unique_key``) ficam de fora por
    exigirem semânticas próprias.

    Args:
        df: DataFrame a avaliar.
        rules: Regras a aplicar; ``None`` retorna status ``NOT_CONFIGURED``.
        run_id: Identificador da execução (para futura correlação).
        target: Nome qualificado do destino (para futura correlação).

    Returns:
        Tupla ``(status, failed_rules, valid_df, quarantined_df, quarantined_count)``:

        - ``status``: ``"NOT_CONFIGURED"`` | ``"PASSED"`` | ``"FAILED"``.
        - ``failed_rules``: lista de dicts ``{rule_name, failed_count, details}``.
        - ``valid_df``: DataFrame sem as linhas quarentenáveis.
        - ``quarantined_df``: linhas que violaram regras isoláveis.
        - ``quarantined_count``: tamanho da quarentena.

    Raises:
        ValueError: se alguma coluna citada não existir, ou se
            ``accepted_values`` ultrapassar ``CONFIG.max_inline_accepted_values``.
    """
    if rules is None:
        return "NOT_CONFIGURED", [], df, df.limit(0), 0

    failed_rules: List[Dict[str, Any]] = []

    if rules.required_columns:
        missing = [c for c in rules.required_columns if c not in df.columns]
        if missing:
            failed_rules.append(
                {
                    "rule_name": "required_columns",
                    "severity": "abort",
                    "status": "FAILED",
                    "failed_count": len(missing),
                    "details": {"missing": missing},
                    "message": "Colunas obrigatórias ausentes.",
                }
            )

    for c in rules.not_null:
        validate_cols(df, [c], "quality.not_null")
    for c in rules.accepted_values.keys():
        validate_cols(df, [c], "quality.accepted_values")
        if len(rules.accepted_values[c]) > CONFIG.max_inline_accepted_values:
            raise ValueError(
                f"quality.accepted_values.{c} possui {len(rules.accepted_values[c])} valores. "
                "Use uma tabela de referência e valide via join para listas grandes."
            )
    for c in rules.max_null_ratio.keys():
        validate_cols(df, [c], "quality.max_null_ratio")
    expression_names = set()
    for rule in rules.expressions:
        if not rule.name or not rule.expression:
            raise ValueError("quality.expressions requer name e expression")
        if rule.name in expression_names:
            raise ValueError(f"quality.expressions possui name duplicado: {rule.name}")
        expression_names.add(rule.name)

    agg_exprs = [F.count(F.lit(1)).alias("__total_rows")]
    null_alias_map: Dict[str, str] = {}
    accepted_alias_map: Dict[str, str] = {}
    expression_alias_map: Dict[str, str] = {}
    expression_condition_map: Dict[str, Any] = {}

    null_cols_needed = set(rules.not_null) | set(rules.max_null_ratio.keys())
    for c in null_cols_needed:
        alias = _safe_agg_alias("nulls", c)
        null_alias_map[c] = alias
        agg_exprs.append(F.sum(F.col(c).isNull().cast("long")).alias(alias))

    for c, values in rules.accepted_values.items():
        alias = _safe_agg_alias("accepted_invalid", c)
        accepted_alias_map[c] = alias
        agg_exprs.append(
            F.sum(
                ((~F.col(c).isin(values)) & F.col(c).isNotNull()).cast("long")
            ).alias(alias)
        )

    for rule in rules.expressions:
        expr_col = F.expr(rule.expression)
        invalid_condition = expr_col.isNull() | (expr_col == F.lit(False))
        alias = _safe_agg_alias("expression_invalid", rule.name)
        expression_alias_map[rule.name] = alias
        expression_condition_map[rule.name] = invalid_condition
        agg_exprs.append(F.sum(invalid_condition.cast("long")).alias(alias))

    if len(agg_exprs) > 1 or rules.min_rows is not None:
        agg_row = df.agg(*agg_exprs).collect()[0]
    else:
        agg_row = None

    row_count = _row_int(agg_row, "__total_rows") if agg_row is not None else df.count()

    quarantine_condition = F.lit(False)

    for c in rules.not_null:
        cnt = _row_int(agg_row, null_alias_map[c])
        if cnt:
            failed_rules.append(
                {
                    "rule_name": f"not_null:{c}",
                    "severity": "quarantine",
                    "status": "FAILED",
                    "failed_count": cnt,
                    "details": {"column": c},
                    "message": f"Coluna {c} contém valores nulos.",
                }
            )
            quarantine_condition = quarantine_condition | F.col(c).isNull()

    for c, values in rules.accepted_values.items():
        cnt = _row_int(agg_row, accepted_alias_map[c])
        if cnt:
            failed_rules.append(
                {
                    "rule_name": f"accepted_values:{c}",
                    "severity": "quarantine",
                    "status": "FAILED",
                    "failed_count": cnt,
                    "details": {"column": c, "values": values},
                    "message": f"Coluna {c} contém valores fora da lista permitida.",
                }
            )
            quarantine_condition = quarantine_condition | (
                ~F.col(c).isin(values) & F.col(c).isNotNull()
            )

    for c, max_ratio in rules.max_null_ratio.items():
        null_count = _row_int(agg_row, null_alias_map[c])
        ratio = 0.0 if row_count == 0 else null_count / row_count
        if ratio > max_ratio:
            failed_rules.append(
                {
                    "rule_name": f"max_null_ratio:{c}",
                    "severity": "quarantine",
                    "status": "FAILED",
                    "failed_count": null_count,
                    "details": {"column": c, "ratio": ratio, "max_ratio": max_ratio},
                    "message": f"Coluna {c} excedeu a razão máxima de nulos.",
                }
            )
            quarantine_condition = quarantine_condition | F.col(c).isNull()

    for rule in rules.expressions:
        invalid_count = _row_int(agg_row, expression_alias_map[rule.name])
        if invalid_count:
            failed_rules.append(
                {
                    "rule_name": f"expression:{rule.name}",
                    "severity": rule.severity,
                    "status": "WARNED" if rule.severity == "warn" else "FAILED",
                    "failed_count": invalid_count,
                    "details": {
                        "name": rule.name,
                        "expression": rule.expression,
                        "severity": rule.severity,
                    },
                    "message": rule.message,
                }
            )
            if rule.severity == "quarantine":
                quarantine_condition = quarantine_condition | expression_condition_map[rule.name]

    for rule_name, rule_config in rules.custom.items():
        rule_type = str(rule_config.get("type") or "").strip()
        evaluator = QUALITY_RULE_REGISTRY.get(rule_type)
        if evaluator is None:
            raise ValueError(f"quality_rules.custom.{rule_name} usa type não registrado: {rule_type}")
        result = evaluator(df, rule_name, dict(rule_config))
        failed_count = int(result.get("failed_count", 0) or 0)
        severity = str(result.get("severity") or rule_config.get("severity") or "abort").strip()
        if severity not in VALID_QUALITY_RULE_SEVERITIES:
            raise ValueError(
                f"quality_rules.custom.{rule_name}.severity={severity!r} não é suportado. "
                f"Valores válidos: {sorted(VALID_QUALITY_RULE_SEVERITIES)}"
            )
        if failed_count:
            failed_rules.append(
                {
                    "rule_name": f"custom:{rule_name}",
                    "severity": severity,
                    "status": "WARNED" if severity == "warn" else "FAILED",
                    "failed_count": failed_count,
                    "details": {
                        "name": rule_name,
                        "type": rule_type,
                        **dict(result.get("details") or {}),
                    },
                    "message": result.get("message") or rule_config.get("message"),
                }
            )
            if severity == "quarantine":
                condition = result.get("condition")
                if condition is None:
                    raise ValueError(
                        f"quality_rules.custom.{rule_name} com severity=quarantine deve retornar condition"
                    )
                quarantine_condition = quarantine_condition | condition

    if rules.unique_key:
        validate_cols(df, rules.unique_key, "quality.unique_key")
        dup_count = (
            df.groupBy(*rules.unique_key)
            .count()
            .where(F.col("count") > 1)
            .count()
        )
        if dup_count:
            failed_rules.append(
                {
                    "rule_name": "unique_key",
                    "severity": "abort",
                    "status": "FAILED",
                    "failed_count": dup_count,
                    "details": {"columns": rules.unique_key},
                    "message": "Chave única possui duplicidades.",
                }
            )

    if rules.min_rows is not None and row_count < rules.min_rows:
        failed_rules.append(
            {
                "rule_name": "min_rows",
                "severity": "abort",
                "status": "FAILED",
                "failed_count": rules.min_rows - row_count,
                "details": {"min_rows": rules.min_rows, "actual": row_count},
                "message": "Quantidade mínima de linhas não atingida.",
            }
        )

    quarantined_df = df.where(quarantine_condition) if failed_rules else df.limit(0)
    quarantined_count = quarantined_df.count() if failed_rules else 0
    valid_df = df.where(~quarantine_condition) if quarantined_count > 0 else df
    if not failed_rules:
        status = "PASSED"
    elif all(r.get("severity") == "warn" for r in failed_rules):
        status = "WARNED"
    else:
        status = "FAILED"
    return status, failed_rules, valid_df, quarantined_df, quarantined_count


def write_quality_results(
    tables: Dict[str, str],
    run_id: str,
    target: str,
    results: List[Dict[str, Any]],
) -> None:
    """Persiste falhas de regras na ctrl table ``ctrl_ingestion_quality``.

    Não escreve nada se ``results`` for vazio (status PASSED ou NOT_CONFIGURED).
    """
    if not results:
        return
    rows = [
        (
            run_id,
            target,
            r["rule_name"],
            r.get("status", "FAILED"),
            r.get("severity"),
            int(r.get("failed_count", 0)),
            utc_now_str(),
            r.get("message"),
            to_json(r.get("details", {})),
        )
        for r in results
    ]
    df = spark.createDataFrame(
        rows,
        "run_id string, target_table string, rule_name string, status string, "
        "severity string, failed_count long, checked_at_utc string, message string, details_json string",
    )
    df = df.withColumn("checked_at_utc", F.col("checked_at_utc").cast("timestamp"))
    df.write.format("delta").mode("append").saveAsTable(tables["quality"])


def write_quarantine(
    tables: Dict[str, str],
    df: DataFrame,
    run_id: str,
    target: str,
    rule_name: str,
    reason: str,
) -> None:
    """Persiste linhas quarentenadas em ``ctrl_ingestion_quarantine``.

    Cada linha vira uma entrada com a linha original serializada em
    ``record_payload`` (JSON) — preserva todos os campos para auditoria
    independente da evolução do schema do destino.
    """
    if df.limit(1).count() == 0:
        return
    payload_df = (
        df.withColumn("run_id", F.lit(run_id))
        .withColumn("target_table", F.lit(target))
        .withColumn("rule_name", F.lit(rule_name))
        .withColumn("error_reason", F.lit(reason))
        .withColumn(
            "record_payload",
            F.to_json(F.struct(*[F.col(c) for c in df.columns])),
        )
        .withColumn("quarantined_at_utc", F.current_timestamp())
        .select(
            "run_id",
            "target_table",
            "rule_name",
            "error_reason",
            "record_payload",
            "quarantined_at_utc",
        )
    )
    payload_df.write.format("delta").mode("append").saveAsTable(tables["quarantine"])
