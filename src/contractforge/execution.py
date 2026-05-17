"""Orquestração declarativa de janelas de execução, backfill e catchup."""
from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._sql import new_run_id, q
from .plan import ExecutionWindow, IngestionPlan, target_full_table_name
from .state import ctrl_table_names, ensure_ctrl_tables
from .watermark import get_watermark

_DURATION_RE = re.compile(r"^\s*(\d+)\s*(hour|hours|day|days|week|weeks)\s*$", re.IGNORECASE)
_SIMPLE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_datetime(value: str, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{field} não pode ser vazio")
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field} deve estar em formato ISO-8601: {value!r}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_duration(value: str) -> timedelta:
    match = _DURATION_RE.match(str(value or ""))
    if not match:
        raise ValueError("execution.window.every deve usar formato como '1 hour', '1 day' ou '1 week'")
    amount = int(match.group(1))
    unit = match.group(2).lower()
    if amount <= 0:
        raise ValueError("execution.window.every deve ser positivo")
    if unit.startswith("hour"):
        return timedelta(hours=amount)
    if unit.startswith("day"):
        return timedelta(days=amount)
    return timedelta(weeks=amount)


def build_time_windows(start: str, end: str, every: str) -> List[ExecutionWindow]:
    """Gera janelas `[start, end)` a partir de limites ISO e duração simples."""
    current = _parse_datetime(start, "execution.window.start")
    final = _parse_datetime(end, "execution.window.end")
    step = _parse_duration(every)
    if current >= final:
        raise ValueError("execution.window.start deve ser menor que execution.window.end")
    windows: List[ExecutionWindow] = []
    while current < final:
        next_value = min(current + step, final)
        start_text = _format_datetime(current)
        end_text = _format_datetime(next_value)
        label = f"{start_text.replace(' ', 'T')}__{end_text.replace(' ', 'T')}"
        windows.append(ExecutionWindow(start=start_text, end=end_text, label=label))
        current = next_value
    return windows


def _watermark_start(raw: Optional[str], column: str) -> Optional[str]:
    if not raw:
        return None
    parsed = json.loads(raw)
    if column not in parsed:
        raise ValueError(f"Watermark não contém a coluna de catchup {column!r}")
    item = parsed[column]
    if not isinstance(item, dict):
        raise ValueError(f"Watermark inválido para coluna {column!r}")
    value = item.get("value")
    return None if value is None else str(value)


def _resolve_windows(plan: IngestionPlan, tables: Dict[str, str]) -> tuple[str, bool, List[ExecutionWindow]]:
    if not plan.execution:
        return "", True, []
    if plan.execution.window:
        config = plan.execution.window
        windows = config.windows or build_time_windows(config.start or "", config.end or "", config.every or "")
        return config.column, config.stop_on_failure, windows
    if plan.execution.catchup and plan.execution.catchup.enabled:
        config = plan.execution.catchup
        column = config.column or ""
        start = config.start
        if not start:
            wm = get_watermark(tables["state"], target_full_table_name(plan), plan.watermark_columns)
            start = _watermark_start(wm, column)
        if not start:
            raise ValueError("execution.catchup sem start exige watermark anterior em ctrl_ingestion_state")
        windows = build_time_windows(start, config.end or "", config.every or "")
        return column, config.stop_on_failure, windows
    return "", True, []


def _window_filter(column: str, window: ExecutionWindow) -> str:
    if not _SIMPLE_COLUMN_RE.match(column):
        raise ValueError("execution.window.column deve ser um nome de coluna simples")
    start = _format_datetime(_parse_datetime(window.start, "execution.window.start"))
    end = _format_datetime(_parse_datetime(window.end, "execution.window.end"))
    return f"(CAST({q(column)} AS TIMESTAMP) >= CAST('{start}' AS TIMESTAMP) AND CAST({q(column)} AS TIMESTAMP) < CAST('{end}' AS TIMESTAMP))"


def _combine_filter(existing: Optional[str], window_expr: str) -> str:
    if existing and str(existing).strip():
        return f"({existing}) AND {window_expr}"
    return window_expr


def _window_label(window: ExecutionWindow, index: int) -> str:
    return window.label or f"window-{index:04d}"


def _child_plan(plan: IngestionPlan, parent_run_id: str, column: str, window: ExecutionWindow, index: int) -> IngestionPlan:
    label = _window_label(window, index)
    runtime_parameters = dict(plan.runtime_parameters or {})
    runtime_parameters["_contractforge_window_label"] = label
    runtime_parameters["_contractforge_window_column"] = column
    runtime_parameters["_contractforge_window_start"] = window.start
    runtime_parameters["_contractforge_window_end"] = window.end
    idempotency_key = f"{plan.idempotency_key}:window:{label}" if plan.idempotency_key else None
    return replace(
        plan,
        execution=None,
        filter_expression=_combine_filter(plan.filter_expression, _window_filter(column, window)),
        parent_run_id=parent_run_id,
        idempotency_key=idempotency_key,
        runtime_parameters=runtime_parameters,
    )


def ingest_execution_plan(plan: IngestionPlan) -> Dict[str, Any]:
    """Executa um plano em múltiplas janelas, delegando cada janela a `ingest_plan`.

    O run pai é lógico: ele coordena e retorna o agregado. Cada janela vira um
    run filho real em `ctrl_ingestion_runs` com `parent_run_id`.
    """
    from .ingestion import ingest_plan

    parent_run_id = new_run_id()
    tables = ctrl_table_names(plan.catalog, plan.ctrl_schema) if plan.dry_run else ensure_ctrl_tables(plan.catalog, plan.ctrl_schema)
    column, stop_on_failure, windows = _resolve_windows(plan, tables)
    if not windows:
        raise ValueError("execution não gerou nenhuma janela")

    results = []
    status = "SUCCESS"
    for index, window in enumerate(windows, start=1):
        child = _child_plan(plan, parent_run_id, column, window, index)
        result = ingest_plan(child)
        result["execution_window"] = {
            "label": _window_label(window, index),
            "column": column,
            "start": window.start,
            "end": window.end,
        }
        results.append(result)
        if result.get("status") == "FAILED":
            status = "FAILED"
            if stop_on_failure:
                break

    return {
        "status": status,
        "run_id": parent_run_id,
        "parent_run_id": parent_run_id,
        "target_table": target_full_table_name(plan),
        "mode": plan.mode,
        "windows_total": len(windows),
        "windows_processed": len(results),
        "windows_succeeded": sum(1 for item in results if item.get("status") in {"SUCCESS", "DRY_RUN", "SKIPPED"}),
        "windows_failed": sum(1 for item in results if item.get("status") == "FAILED"),
        "rows_read": sum(int(item.get("rows_read") or 0) for item in results),
        "rows_written": sum(int(item.get("rows_written") or 0) for item in results),
        "rows_quarantined": sum(int(item.get("rows_quarantined") or 0) for item in results),
        "window_results": results,
    }
