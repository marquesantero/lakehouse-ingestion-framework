# Databricks notebook source
from typing import Any, Callable, cast

from lakehouse_ingestion import ingest_bundle, load_contract_bundle

dbutils_obj = globals().get("dbutils")
if dbutils_obj is None:
    raise RuntimeError("Este notebook deve ser executado em Databricks com dbutils disponível")

dbutils_typed = cast(Any, dbutils_obj)
dbutils_typed.widgets.text("contract", "")
contract = dbutils_typed.widgets.get("contract")

if not contract:
    raise ValueError("Widget 'contract' é obrigatório")

bundle = load_contract_bundle(contract)
result = ingest_bundle(bundle)

display_fn = cast(Callable[[object], None], globals().get("display", print))
display_fn(result)

