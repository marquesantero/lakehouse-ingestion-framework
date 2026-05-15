"""Testes do módulo de qualidade (consolidação de agregações + quarentena)."""
from __future__ import annotations

import pytest

from contractforge import QualityExpression, QualityRules
from contractforge.quality import (
    ABORT_ONLY_RULES,
    evaluate_quality,
    is_abort_only_failure,
)


def _by_rule(failed):
    return {r["rule_name"]: r for r in failed}


def test_not_null_passes_when_complete(make_df):
    df = make_df(
        [(1, "ok"), (2, "ok2")],
        "id long, name string",
    )
    rules = QualityRules(not_null=["id", "name"])
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    assert status == "PASSED"
    assert failed == []
    assert q_count == 0


def test_not_null_quarantines_offending_rows(make_df):
    df = make_df(
        [(1, "ok"), (2, None), (3, "ok"), (None, "x")],
        "id long, name string",
    )
    rules = QualityRules(not_null=["id", "name"])
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    by_rule = _by_rule(failed)
    assert by_rule["not_null:id"]["failed_count"] == 1
    assert by_rule["not_null:name"]["failed_count"] == 1
    assert q_count == 2  # row id=2 e row id=None
    assert valid.count() == 2


def test_unique_key_detects_duplicates(make_df):
    df = make_df(
        [(1, "a"), (1, "b"), (2, "c")],
        "id long, val string",
    )
    rules = QualityRules(unique_key=["id"])
    status, failed, *_ = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    assert _by_rule(failed)["unique_key"]["failed_count"] == 1


def test_accepted_values(make_df):
    df = make_df(
        [(1, "A"), (2, "B"), (3, "X"), (4, None)],
        "id long, status string",
    )
    rules = QualityRules(accepted_values={"status": ["A", "B"]})
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    assert _by_rule(failed)["accepted_values:status"]["failed_count"] == 1
    assert q_count == 1
    assert valid.count() == 3  # NULL não é inválido aqui


def test_min_rows(make_df):
    df = make_df([(1,)], "id long")
    rules = QualityRules(min_rows=5)
    status, failed, *_ = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    assert _by_rule(failed)["min_rows"]["failed_count"] == 4


def test_max_null_ratio(make_df):
    df = make_df(
        [(1, None), (2, None), (3, "x"), (4, "y")],
        "id long, val string",
    )
    rules = QualityRules(max_null_ratio={"val": 0.4})
    status, failed, *_ = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    rule = _by_rule(failed)["max_null_ratio:val"]
    assert rule["details"]["ratio"] == 0.5
    assert rule["details"]["max_ratio"] == 0.4


def test_required_columns_missing(make_df):
    df = make_df([(1,)], "id long")
    rules = QualityRules(required_columns=["id", "ghost"])
    status, failed, *_ = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    assert _by_rule(failed)["required_columns"]["details"]["missing"] == ["ghost"]


def test_accepted_values_too_large_rejected(make_df):
    """Listas grandes devem forçar uso de tabela de referência."""
    df = make_df([(1, "a")], "id long, status string")
    rules = QualityRules(accepted_values={"status": [str(i) for i in range(2000)]})
    with pytest.raises(ValueError, match="tabela de referência"):
        evaluate_quality(df, rules, "r1", "t")


def test_no_rules_returns_not_configured(make_df):
    df = make_df([(1,)], "id long")
    status, failed, valid, quarantined, q_count = evaluate_quality(df, None, "r1", "t")
    assert status == "NOT_CONFIGURED"
    assert failed == []
    assert q_count == 0


def test_abort_only_rules_classification():
    """Regras de conjunto não conseguem isolar linhas — devem ser classificadas
    como abortivas."""
    assert ABORT_ONLY_RULES == frozenset({"required_columns", "unique_key", "min_rows"})
    assert is_abort_only_failure("required_columns") is True
    assert is_abort_only_failure("unique_key") is True
    assert is_abort_only_failure("min_rows") is True
    assert is_abort_only_failure("expression:valid_period") is False
    assert is_abort_only_failure("not_null:col1") is False
    assert is_abort_only_failure("accepted_values:status") is False
    assert is_abort_only_failure("max_null_ratio:val") is False
    assert is_abort_only_failure("expression:positive_amount") is False


def test_expression_rule_quarantines_invalid_rows(make_df):
    df = make_df(
        [(1, 10), (2, 0), (3, None)],
        "id long, amount long",
    )
    rules = QualityRules(expressions=[QualityExpression(name="positive_amount", expression="amount > 0")])
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    assert _by_rule(failed)["expression:positive_amount"]["failed_count"] == 2
    assert q_count == 2
    assert valid.count() == 1


def test_expression_rule_warns_without_quarantine(make_df):
    df = make_df(
        [(1, 10), (2, 0)],
        "id long, amount long",
    )
    rules = QualityRules(
        expressions=[
            QualityExpression(
                name="positive_amount",
                expression="amount > 0",
                severity="warn",
                message="amount should be positive",
            )
        ]
    )
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    rule = _by_rule(failed)["expression:positive_amount"]
    assert status == "WARNED"
    assert rule["status"] == "WARNED"
    assert rule["severity"] == "warn"
    assert rule["message"] == "amount should be positive"
    assert q_count == 0
    assert valid.count() == 2


def test_combined_rules_single_pass(make_df):
    """Múltiplas regras de coluna devem ser avaliadas em uma agregação consolidada."""
    df = make_df(
        [(1, "A", None), (2, None, "ok"), (3, "Z", "ok"), (None, "A", "ok")],
        "id long, status string, name string",
    )
    rules = QualityRules(
        not_null=["id", "status", "name"],
        accepted_values={"status": ["A", "B"]},
        max_null_ratio={"name": 0.1},
    )
    status, failed, valid, quarantined, q_count = evaluate_quality(df, rules, "r1", "t")
    assert status == "FAILED"
    by = _by_rule(failed)
    assert by["not_null:id"]["failed_count"] == 1
    assert by["not_null:status"]["failed_count"] == 1
    assert by["not_null:name"]["failed_count"] == 1
    assert by["accepted_values:status"]["failed_count"] == 1  # "Z"
    assert "max_null_ratio:name" in by  # 1/4 = 0.25 > 0.1
