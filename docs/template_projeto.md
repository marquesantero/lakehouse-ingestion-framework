# Template de Projeto

O diretório `examples/project_template/` contém uma estrutura mínima para iniciar um projeto ContractForge com Databricks Asset Bundles.

Para exemplos mais completos e validáveis por CLI, veja também `examples/playground/`.

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
4. Para criar novos contratos, use `contractforge init --output contracts/silver/c_orders --source bronze.b_orders --target-table c_orders --layer silver --mode scd1_upsert --merge-keys order_id --split`.
5. Execute `contractforge validate-project contracts` localmente/CI.
6. Execute o notebook genérico passando o path do contrato como parâmetro.

## Playground

O diretório `examples/playground/` contém contratos prontos para cenários comuns:

- REST API incremental para Bronze.
- Auto Loader JSON `available_now`.
- JDBC incremental com SCD1.
- Snapshot com soft delete.
- Histórico SCD2.
- Gold full refresh de KPI.

Valide todos os exemplos:

```bash
python examples/playground/scripts/validate_playground.py
```

Ou:

```bash
contractforge validate-project examples/playground/contracts
```

## Princípio

O notebook deve ser genérico. A lógica de ingestão fica no YAML:

```python
from contractforge import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle(dbutils.widgets.get("contract"))
result = ingest_bundle(bundle)
display(result)
```

Isso evita notebooks por tabela e deixa mudanças de pipeline revisáveis em pull request.

