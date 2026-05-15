"""Testes de watermark simples e composto."""
from __future__ import annotations

import json

from contractforge.watermark import (
    apply_watermark,
    compute_watermark,
    decode_watermark,
    encode_watermark,
)


def test_encode_decode_roundtrip(make_df):
    df = make_df([(1, "2024-01-01")], "id long, updated_at string")
    encoded = encode_watermark(df, {"updated_at": "2024-01-15"})
    decoded = decode_watermark(encoded, ["updated_at"])
    assert decoded == {"updated_at": {"type": "string", "value": "2024-01-15"}}


def test_compute_watermark_simple(make_df):
    df = make_df(
        [("2024-01-01",), ("2024-01-15",), ("2024-01-10",)],
        "updated_at string",
    )
    encoded = compute_watermark(df, ["updated_at"])
    payload = json.loads(encoded)
    assert payload["updated_at"]["value"] == "2024-01-15"


def test_compute_watermark_composite(make_df):
    df = make_df(
        [("2024-01-01", 1), ("2024-01-15", 2), ("2024-01-15", 3), ("2024-01-15", 1)],
        "updated_at string, version long",
    )
    encoded = compute_watermark(df, ["updated_at", "version"])
    payload = json.loads(encoded)
    assert payload["updated_at"]["value"] == "2024-01-15"
    assert payload["version"]["value"] == "3"


def test_apply_watermark_filters_simple(make_df):
    df = make_df(
        [(1, "2024-01-01"), (2, "2024-01-10"), (3, "2024-01-15")],
        "id long, updated_at string",
    )
    last = encode_watermark(df, {"updated_at": "2024-01-05"})
    filtered = apply_watermark(df, ["updated_at"], last)
    ids = sorted(r[0] for r in filtered.collect())
    assert ids == [2, 3]


def test_apply_watermark_returns_input_when_empty(make_df):
    df = make_df([(1,)], "id long")
    assert apply_watermark(df, [], None) is df
    assert apply_watermark(df, ["id"], None) is df


def test_apply_watermark_composite(make_df):
    df = make_df(
        [(1, "2024-01-01", 1),
         (2, "2024-01-10", 1),
         (3, "2024-01-10", 2),
         (4, "2024-01-15", 1)],
        "id long, updated_at string, version long",
    )
    last = encode_watermark(df, {"updated_at": "2024-01-10", "version": 1})
    filtered = apply_watermark(df, ["updated_at", "version"], last)
    ids = sorted(r[0] for r in filtered.collect())
    # mantém: (updated_at == 2024-01-10 e version > 1) OR (updated_at > 2024-01-10)
    assert ids == [3, 4]
