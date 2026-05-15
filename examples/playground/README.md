# ContractForge Playground

Projeto de exemplo para explorar o ContractForge sem depender de fontes externas reais.

O objetivo é mostrar contratos completos, validar estrutura e servir como base para copiar cenários para projetos reais.

## Cenários

```text
contracts/
  bronze/
    b_orders_api.*              # REST API incremental
    b_orders_files.*            # Auto Loader JSON available_now
  silver/
    s_orders.*                  # JDBC incremental + SCD1
    s_devices.*                 # snapshot_soft_delete
    s_customers_history.*       # SCD2
  gold/
    g_daily_orders.*            # Gold full refresh KPI
notebooks/
  run_contract.py               # notebook genérico para Databricks
scripts/
  validate_playground.py        # valida todos os contratos via CLI
```

## Validação Local

Instale a lib em modo desenvolvimento:

```bash
pip install -e ".[dev]"
```

Valide o playground:

```bash
python examples/playground/scripts/validate_playground.py
```

Ou diretamente:

```bash
contractforge validate-project examples/playground/contracts
contractforge governance-preview examples/playground/contracts/silver/s_orders
contractforge templates list
```

## Uso Em Projeto Real

1. Copie o cenário mais próximo para seu repositório.
2. Ajuste `target.catalog`, `target.schema`, `target.table`.
3. Troque URLs, paths, names e secrets.
4. Revise `quality_rules`, `operations` e `access`.
5. Rode `contractforge validate-bundle`.
6. Execute com o notebook genérico ou com uma chamada `ingest_bundle()`.

## Observação

Os contratos são exemplos de estrutura e governança. Eles não devem ser executados como ingestão real sem ajustar fontes, credenciais, schemas, permissões e paths.
