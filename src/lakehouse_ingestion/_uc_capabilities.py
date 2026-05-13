"""Deteccao conservadora de capacidades Unity Catalog.

O objetivo aqui e falhar cedo quando o contrato declara recursos que dependem
de Unity Catalog, sem criar objetos temporarios nem executar probes destrutivos.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class UCCapabilities:
    table_comments: bool = True
    column_comments: bool = True
    table_tags: bool = False
    column_tags: bool = False
    grants: bool = True
    row_filters: bool = False
    column_masks: bool = False

    def as_dict(self) -> Dict[str, bool]:
        return {
            "table_comments": self.table_comments,
            "column_comments": self.column_comments,
            "table_tags": self.table_tags,
            "column_tags": self.column_tags,
            "grants": self.grants,
            "row_filters": self.row_filters,
            "column_masks": self.column_masks,
        }


def _is_three_part_name(target_table: str) -> bool:
    return len([part for part in target_table.split(".") if part.strip()]) >= 3


def get_uc_capabilities(target_table: Optional[str] = None) -> UCCapabilities:
    """Retorna capacidades esperadas para o alvo.

    Tags, row filters e masks sao tratados como recursos de Unity Catalog e
    exigem nome de tabela em tres partes: ``catalog.schema.table``.
    """
    is_uc_target = bool(target_table and _is_three_part_name(target_table))
    return UCCapabilities(
        table_tags=is_uc_target,
        column_tags=is_uc_target,
        row_filters=is_uc_target,
        column_masks=is_uc_target,
    )


def capability_issues(
    target_table: str,
    requirements: Iterable[tuple[str, str, str, str]],
    *,
    capabilities: Optional[UCCapabilities] = None,
) -> List[Dict[str, Any]]:
    """Gera issues para capacidades declaradas mas nao suportadas."""
    caps = capabilities or get_uc_capabilities(target_table)
    caps_dict = caps.as_dict()
    issues = []
    for capability, scope, obj, severity in requirements:
        if caps_dict.get(capability, False):
            continue
        issues.append(
            {
                "severity": severity,
                "scope": scope,
                "object": obj,
                "message": (
                    f"{capability} nao e suportado para {target_table}. "
                    "Use tabela Unity Catalog em tres partes ou remova o recurso do contrato."
                ),
            }
        )
    return issues
