# Databricks notebook source
# ruff: noqa: E402,F821
dbutils.widgets.text("contract", "")
contract_path = dbutils.widgets.get("contract")

if not contract_path:
    raise ValueError("Informe o parametro 'contract' com o path base do bundle ou arquivo ingestion.")

from contractforge import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle(contract_path)
result = ingest_bundle(bundle)
display(result)
