from __future__ import annotations

import pytest

from contractforge import ContractForgeExecutionError
from contractforge.exceptions import raise_for_failure_result


def test_raise_for_failure_result_raises_execution_error():
    result = {
        "status": "FAILED",
        "run_id": "run-1",
        "target_table": "main.silver.orders",
        "error_message": "quality gate failed",
    }

    with pytest.raises(ContractForgeExecutionError) as exc:
        raise_for_failure_result(result)

    assert exc.value.result == result
    assert exc.value.status == "FAILED"
    assert exc.value.run_id == "run-1"
    assert exc.value.target_table == "main.silver.orders"
    assert exc.value.error_message == "quality gate failed"
    assert "quality gate failed" in str(exc.value)


def test_raise_for_failure_result_ignores_non_failure_statuses():
    raise_for_failure_result({"status": "SUCCESS"})
    raise_for_failure_result({"status": "SKIPPED"})
    raise_for_failure_result({"status": "DRY_RUN"})

