# Databricks notebook source
dbutils.widgets.text("contract", "")
contract = dbutils.widgets.get("contract")

if not contract:
    raise ValueError("Widget 'contract' é obrigatório")

from lakehouse_ingestion import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle(contract)
result = ingest_bundle(bundle)

display(result)

