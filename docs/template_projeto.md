# Template de Projeto

O diretório `examples/project_template/` contém uma estrutura mínima para iniciar um projeto ContractForge com Databricks Asset Bundles.

## Estrutura

```text
examples/project_template/
  README.md
  databricks.yml
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
```

## Uso esperado

1. Copie o template para um novo repositório de dados.
2. Ajuste `bundle.name`, `workspace.root_path`, `catalog`, schemas e paths.
3. Publique o wheel do ContractForge em um Volume ou registry.
4. Execute `contractforge validate-project contracts` localmente/CI.
5. Execute o notebook genérico passando o path do contrato como parâmetro.

## Princípio

O notebook deve ser genérico. A lógica de ingestão fica no YAML:

```python
from lakehouse_ingestion import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle(dbutils.widgets.get("contract"))
result = ingest_bundle(bundle)
display(result)
```

Isso evita notebooks por tabela e deixa mudanças de pipeline revisáveis em pull request.

