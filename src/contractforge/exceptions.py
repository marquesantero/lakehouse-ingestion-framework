"""Public ContractForge exceptions."""
from __future__ import annotations

from typing import Any, Mapping

FAILURE_STATUSES = {"FAILED", "ABORTED"}


class ContractForgeExecutionError(RuntimeError):
    """Raised when an ingestion API returns a failed execution result."""

    def __init__(self, result: Mapping[str, Any]) -> None:
        self.result = dict(result)
        self.status = str(result.get("status") or "UNKNOWN")
        self.run_id = result.get("run_id") or result.get("stream_run_id")
        self.target_table = result.get("target_table")
        self.error_message = result.get("error_message")
        target = self.target_table or "unknown target"
        run = f", run_id={self.run_id}" if self.run_id else ""
        message = self.error_message or f"Execution returned status {self.status}"
        super().__init__(
            f"ContractForge ingestion failed for {target} "
            f"(status={self.status}{run}): {message}"
        )


def raise_for_failure_result(result: Mapping[str, Any]) -> None:
    """Raise ``ContractForgeExecutionError`` when a result represents failure."""
    if str(result.get("status") or "").upper() in FAILURE_STATUSES:
        raise ContractForgeExecutionError(result)
