"""Analise operacional de custo e eficiencia baseada nas ctrl tables."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ._spark import spark
from ._sql import q, qt, sql_lit
from .state import ctrl_table_names


VALID_COST_GROUP_FIELDS = {
    "target_table",
    "layer",
    "mode",
    "status",
    "contract_domain",
    "contract_owner",
    "criticality",
    "runtime_type",
    "source_connector",
    "source_provider",
}

DEFAULT_COST_GROUP_BY = ("target_table", "layer", "mode", "status")


@dataclass(frozen=True)
class CostModel:
    """Parametros para estimativa logica de custo.

    O modelo nao representa faturamento real do provedor. Ele aplica uma taxa
    informada pelo usuario sobre a duracao registrada em ``ctrl_ingestion_runs``.
    """

    dbu_per_hour: float | None = None
    currency_per_dbu: float | None = None
    currency: str = "USD"

    @property
    def enabled(self) -> bool:
        return self.dbu_per_hour is not None and self.currency_per_dbu is not None

    @property
    def hourly_rate(self) -> float | None:
        if not self.enabled:
            return None
        return float(self.dbu_per_hour or 0.0) * float(self.currency_per_dbu or 0.0)


def _validate_lookback_days(lookback_days: int) -> None:
    if lookback_days < 1:
        raise ValueError("lookback_days deve ser maior ou igual a 1")


def _validate_float(name: str, value: float | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} deve ser maior ou igual a 0")


def _normalize_group_by(group_by: Iterable[str] | None) -> tuple[str, ...]:
    fields = tuple(group_by or DEFAULT_COST_GROUP_BY)
    if not fields:
        raise ValueError("group_by deve conter pelo menos um campo")
    unknown = sorted(set(fields) - VALID_COST_GROUP_FIELDS)
    if unknown:
        raise ValueError(f"group_by desconhecido: {unknown}")
    return fields


def _sql_number(value: float | None) -> str:
    return "NULL" if value is None else repr(float(value))


def _group_select(fields: tuple[str, ...]) -> str:
    return ",\n        ".join(q(field) for field in fields)


def _group_by(fields: tuple[str, ...]) -> str:
    return ", ".join(q(field) for field in fields)


def build_operational_cost_query(
    catalog: str,
    ctrl_schema: str,
    *,
    lookback_days: int = 30,
    group_by: Iterable[str] | None = None,
    cost_model: CostModel | None = None,
    include_failed: bool = True,
) -> str:
    """Monta SQL de analise de custo/eficiencia sobre ``ctrl_ingestion_runs``.

    A funcao e pura: nao executa Spark e nao cria objetos. O resultado e uma
    consulta agregada com throughput, tempo por etapa e custo estimado quando
    ``cost_model`` contem ``dbu_per_hour`` e ``currency_per_dbu``.
    """

    _validate_lookback_days(lookback_days)
    fields = _normalize_group_by(group_by)
    model = cost_model or CostModel()
    _validate_float("dbu_per_hour", model.dbu_per_hour)
    _validate_float("currency_per_dbu", model.currency_per_dbu)

    runs_table = ctrl_table_names(catalog, ctrl_schema)["runs"]
    status_filter = "" if include_failed else "AND status = 'SUCCESS'"
    group_select = _group_select(fields)
    group_by_sql = _group_by(fields)
    hourly_rate = model.hourly_rate
    hourly_rate_sql = _sql_number(hourly_rate)
    currency_sql = sql_lit(model.currency)

    return f"""
WITH base AS (
    SELECT
        run_id,
        run_date,
        target_table,
        layer,
        mode,
        status,
        contract_domain,
        contract_owner,
        runtime_type,
        source_connector,
        source_provider,
        COALESCE(get_json_object(operations_json, '$.criticality'), 'unknown') AS criticality,
        CAST(COALESCE(rows_read, 0) AS BIGINT) AS rows_read,
        CAST(COALESCE(rows_written, 0) AS BIGINT) AS rows_written,
        CAST(COALESCE(rows_quarantined, 0) AS BIGINT) AS rows_quarantined,
        CAST(COALESCE(duration_seconds, 0.0) AS DOUBLE) AS duration_seconds,
        CAST(COALESCE(get_json_object(stage_durations_json, '$.read'), '0') AS DOUBLE) AS read_seconds,
        CAST(COALESCE(get_json_object(stage_durations_json, '$.prepare'), '0') AS DOUBLE) AS prepare_seconds,
        CAST(COALESCE(get_json_object(stage_durations_json, '$.quality'), '0') AS DOUBLE) AS quality_seconds,
        CAST(COALESCE(get_json_object(stage_durations_json, '$.write'), '0') AS DOUBLE) AS write_seconds,
        CAST(COALESCE(get_json_object(stage_durations_json, '$.governance'), '0') AS DOUBLE) AS governance_seconds
    FROM {qt(runs_table)}
    WHERE run_date >= date_sub(current_date(), {int(lookback_days)})
      {status_filter}
),
agg AS (
    SELECT
        {group_select},
        COUNT(*) AS runs,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
        SUM(rows_read) AS rows_read,
        SUM(rows_written) AS rows_written,
        SUM(rows_quarantined) AS rows_quarantined,
        SUM(duration_seconds) AS duration_seconds,
        SUM(read_seconds) AS read_seconds,
        SUM(prepare_seconds) AS prepare_seconds,
        SUM(quality_seconds) AS quality_seconds,
        SUM(write_seconds) AS write_seconds,
        SUM(governance_seconds) AS governance_seconds
    FROM base
    GROUP BY {group_by_sql}
)
SELECT
    *,
    CASE WHEN duration_seconds > 0 THEN rows_written / duration_seconds ELSE NULL END AS rows_written_per_second,
    CASE WHEN duration_seconds > 0 THEN rows_read / duration_seconds ELSE NULL END AS rows_read_per_second,
    CASE WHEN runs > 0 THEN duration_seconds / runs ELSE NULL END AS avg_duration_seconds,
    {hourly_rate_sql} AS estimated_hourly_rate,
    {currency_sql} AS estimated_currency,
    CASE
        WHEN {hourly_rate_sql} IS NULL THEN NULL
        ELSE duration_seconds / 3600.0 * {hourly_rate_sql}
    END AS estimated_compute_cost,
    CASE
        WHEN {hourly_rate_sql} IS NULL OR rows_written <= 0 THEN NULL
        ELSE (duration_seconds / 3600.0 * {hourly_rate_sql}) / (rows_written / 1000000.0)
    END AS estimated_cost_per_million_rows,
    'estimated_from_ctrl_runs' AS cost_source
FROM agg
ORDER BY estimated_compute_cost DESC NULLS LAST, duration_seconds DESC
""".strip()


def operational_cost_dataframe(
    catalog: str,
    ctrl_schema: str,
    *,
    lookback_days: int = 30,
    group_by: Iterable[str] | None = None,
    cost_model: CostModel | None = None,
    include_failed: bool = True,
):
    """Executa a consulta e retorna um DataFrame Spark."""

    query = build_operational_cost_query(
        catalog,
        ctrl_schema,
        lookback_days=lookback_days,
        group_by=group_by,
        cost_model=cost_model,
        include_failed=include_failed,
    )
    return spark.sql(query)


def analyze_operational_cost(
    catalog: str,
    ctrl_schema: str,
    *,
    lookback_days: int = 30,
    group_by: Iterable[str] | None = None,
    cost_model: CostModel | None = None,
    include_failed: bool = True,
    limit: int = 100,
    query_only: bool = False,
) -> dict[str, Any]:
    """Retorna relatorio JSON-friendly de custo/eficiencia operacional."""

    if limit < 1:
        raise ValueError("limit deve ser maior ou igual a 1")
    query = build_operational_cost_query(
        catalog,
        ctrl_schema,
        lookback_days=lookback_days,
        group_by=group_by,
        cost_model=cost_model,
        include_failed=include_failed,
    )
    fields = _normalize_group_by(group_by)
    model = cost_model or CostModel()
    if query_only:
        rows: list[dict[str, Any]] = []
    else:
        rows = [row.asDict(recursive=True) for row in spark.sql(query).limit(int(limit)).collect()]
    return {
        "status": "QUERY_ONLY" if query_only else "SUCCESS",
        "catalog": catalog,
        "ctrl_schema": ctrl_schema,
        "lookback_days": lookback_days,
        "group_by": list(fields),
        "include_failed": include_failed,
        "cost_model": {
            "enabled": model.enabled,
            "dbu_per_hour": model.dbu_per_hour,
            "currency_per_dbu": model.currency_per_dbu,
            "currency": model.currency,
            "hourly_rate": model.hourly_rate,
        },
        "query": query,
        "rows": rows,
    }
