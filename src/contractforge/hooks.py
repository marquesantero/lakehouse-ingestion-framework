"""Hooks programáticos para extensões controladas do fluxo de ingestão."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from .plan import IngestionPlan


BeforeReadHook = Callable[["IngestionPlan"], None]
AfterPrepareHook = Callable[[DataFrame, "IngestionPlan"], DataFrame]
BeforeWriteHook = Callable[[DataFrame, "IngestionPlan"], DataFrame]
AfterWriteHook = Callable[[Dict[str, Any], "IngestionPlan"], None]


@dataclass(frozen=True)
class IngestionHooks:
    """Callbacks opcionais e explícitos para customizações fora do core.

    Hooks que recebem DataFrame devem retornar um DataFrame. Falhas nos hooks
    propagam como falha da ingestão, preservando rastreio nas ctrl tables.
    """

    before_read: Optional[BeforeReadHook] = None
    after_prepare: Optional[AfterPrepareHook] = None
    before_write: Optional[BeforeWriteHook] = None
    after_write: Optional[AfterWriteHook] = None


def normalize_hooks(value: Optional[IngestionHooks]) -> Optional[IngestionHooks]:
    """Valida hooks programáticos recebidos no plano."""
    if value is None:
        return None
    if not isinstance(value, IngestionHooks):
        raise ValueError("hooks deve ser uma instância de IngestionHooks")
    for name in ("before_read", "after_prepare", "before_write", "after_write"):
        hook = getattr(value, name)
        if hook is not None and not callable(hook):
            raise ValueError(f"hooks.{name} deve ser callable")
    return value
