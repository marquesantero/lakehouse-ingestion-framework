# ContractForge Project Template

Template mínimo para um projeto de ingestão declarativa com ContractForge e Databricks Asset Bundles.

## Estrutura

```text
contracts/
  bronze/
    b_orders.ingestion.yaml
  silver/
    c_orders.ingestion.yaml
    c_orders.annotations.yaml
    c_orders.operations.yaml
    c_orders.access.yaml
notebooks/
  run_contract.py
databricks.yml
```

## Fluxo

1. Ajuste `catalog`, schemas, paths e permissões.
2. Instale o wheel versionado do ContractForge no job/cluster.
3. Valide contratos no CI:

```bash
contractforge init --output contracts/bronze/b_orders.ingestion.yaml --source main.raw.orders --target-table b_orders
contractforge validate contracts/bronze/b_orders.ingestion.yaml
contractforge validate-bundle contracts/silver/c_orders
contractforge validate-project contracts
```

4. Execute o notebook genérico passando o parâmetro `contract`.

