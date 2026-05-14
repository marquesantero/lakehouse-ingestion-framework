"""Operacoes de manutencao operacional do ContractForge."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable

from ._spark import spark
from ._sql import q, qt
from .state import ctrl_table_names


@dataclass(frozen=True)
class CtrlRetentionTarget:
    """Tabela de controle historica sujeita a retencao."""

    key: str
    age_expression: str
    description: str


CTRL_RETENTION_TARGETS: tuple[CtrlRetentionTarget, ...] = (
    CtrlRetentionTarget("runs", "run_date", "Historico de execucoes"),
    CtrlRetentionTarget("errors", "error_date", "Stack traces de erro"),
    CtrlRetentionTarget("quality", "checked_at_utc", "Resultados de quality gates"),
    CtrlRetentionTarget("quarantine", "quarantined_at_utc", "Linhas quarentenadas"),
    CtrlRetentionTarget("locks", "COALESCE(released_at_utc, expires_at_utc, acquired_at_utc)", "Locks expirados/liberados"),
    CtrlRetentionTarget("explain", "captured_at_utc", "Planos Spark capturados"),
    CtrlRetentionTarget("lineage", "event_time_utc", "Eventos OpenLineage"),
    CtrlRetentionTarget("schema_changes", "change_ts_utc", "Historico de mudancas de schema"),
    CtrlRetentionTarget("streams", "COALESCE(ended_at_utc, started_at_utc)", "Historico de streams"),
    CtrlRetentionTarget("annotations", "annotation_date", "Auditoria de annotations"),
    CtrlRetentionTarget("operations", "recorded_at_utc", "Auditoria operacional"),
    CtrlRetentionTarget("access", "access_date", "Auditoria de access"),
)


def _validate_retention_days(retention_days: int) -> None:
    if retention_days < 1:
        raise ValueError("retention_days deve ser maior ou igual a 1")


def _validate_vacuum_retention_hours(vacuum_retention_hours: int) -> None:
    if vacuum_retention_hours < 0:
        raise ValueError("vacuum_retention_hours deve ser maior ou igual a 0")


def _cutoff_predicate(age_expression: str, retention_days: int) -> str:
    expression = age_expression.strip()
    if expression.endswith("_date") and "(" not in expression:
        return f"{q(expression)} < date_sub(current_date(), {int(retention_days)})"
    return f"{expression} < current_timestamp() - INTERVAL {int(retention_days)} DAYS"


def build_ctrl_retention_plan(
    catalog: str,
    ctrl_schema: str,
    *,
    retention_days: int,
    vacuum: bool = False,
    vacuum_retention_hours: int = 168,
    targets: Iterable[str] | None = None,
) -> list[Dict[str, Any]]:
    """Gera o plano SQL de retencao para ctrl tables historicas.

    A funcao e pura: nao cria schema, nao le Spark e nao executa SQL. Use
    ``apply_ctrl_retention`` para executar.
    """

    _validate_retention_days(retention_days)
    _validate_vacuum_retention_hours(vacuum_retention_hours)
    requested = set(targets or [])
    known = {target.key for target in CTRL_RETENTION_TARGETS}
    unknown = requested - known
    if unknown:
        raise ValueError(f"targets de ctrl retention desconhecidos: {sorted(unknown)}")

    names = ctrl_table_names(catalog, ctrl_schema)
    plan: list[Dict[str, Any]] = []
    for target in CTRL_RETENTION_TARGETS:
        if requested and target.key not in requested:
            continue
        table = names[target.key]
        predicate = _cutoff_predicate(target.age_expression, retention_days)
        commands = [f"DELETE FROM {qt(table)} WHERE {predicate}"]
        if vacuum:
            commands.append(f"VACUUM {qt(table)} RETAIN {int(vacuum_retention_hours)} HOURS")
        plan.append(
            {
                "target": target.key,
                "table": table,
                "description": target.description,
                "retention_days": retention_days,
                "predicate": predicate,
                "commands": commands,
            }
        )
    return plan


def apply_ctrl_retention(
    catalog: str,
    ctrl_schema: str,
    *,
    retention_days: int,
    vacuum: bool = False,
    vacuum_retention_hours: int = 168,
    dry_run: bool = True,
    targets: Iterable[str] | None = None,
) -> Dict[str, Any]:
    """Aplica, ou apenas simula, retencao nas ctrl tables historicas."""

    plan = build_ctrl_retention_plan(
        catalog,
        ctrl_schema,
        retention_days=retention_days,
        vacuum=vacuum,
        vacuum_retention_hours=vacuum_retention_hours,
        targets=targets,
    )
    executed: list[str] = []
    if not dry_run:
        for item in plan:
            for command in item["commands"]:
                spark.sql(command)
                executed.append(command)
    return {
        "status": "DRY_RUN" if dry_run else "SUCCESS",
        "catalog": catalog,
        "ctrl_schema": ctrl_schema,
        "retention_days": retention_days,
        "vacuum": vacuum,
        "vacuum_retention_hours": vacuum_retention_hours,
        "targets": [item["target"] for item in plan],
        "plan": plan,
        "executed_commands": executed,
    }
