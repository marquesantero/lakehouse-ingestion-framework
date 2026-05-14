# Quick Start em 5 minutos

Este guia mostra o menor fluxo funcional para validar o ContractForge sem montar uma arquitetura completa.

## 1. Instale

No Databricks, instale o wheel versionado no cluster/job:

```bash
%pip install /Volumes/<catalog>/<schema>/libs/contractforge-1.15.0-py3-none-any.whl
```

Para desenvolvimento local com Spark:

```bash
pip install -e ".[dev]"
```

## 2. Crie uma tabela de origem simples

Em um notebook Databricks:

```python
from pyspark.sql import Row

spark.sql("CREATE SCHEMA IF NOT EXISTS main.raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.ops")

df = spark.createDataFrame([
    Row(order_id=1, customer_id=10, amount=120.5, updated_at="2026-05-14T10:00:00Z"),
    Row(order_id=2, customer_id=20, amount=75.0, updated_at="2026-05-14T10:05:00Z"),
])

df.write.mode("overwrite").saveAsTable("main.raw.orders_quickstart")
```

## 3. Execute uma ingestão mínima

```python
from lakehouse_ingestion import ingest

result = ingest(
    source={
        "type": "connector",
        "connector": "table",
        "table": "main.raw.orders_quickstart",
        "read": {"source_complete": True},
    },
    catalog="main",
    layer="bronze",
    target_table="b_orders_quickstart",
    mode="scd0_append",
    schema_policy="additive_only",
    ctrl_schema="ops",
    quality_rules={
        "not_null": ["order_id"],
        "unique_key": ["order_id"],
    },
)

result
```

Resultado esperado:

- `status = SUCCESS`
- `target_table = main.bronze.b_orders_quickstart`
- `rows_read = 2`
- `rows_written = 2`

Por padrão, o schema físico do target é o próprio `layer`. Se sua organização usa schemas de negócio, adicione `target_schema`, por exemplo `target_schema="landing_orders"` para gravar em `main.landing_orders.b_orders_quickstart` mantendo `layer="bronze"` como camada lógica.

## 4. Consulte evidências operacionais

```sql
SELECT run_id, target_table, mode, status, rows_read, rows_written, framework_version
FROM main.ops.ctrl_ingestion_runs
WHERE target_table = 'main.bronze.b_orders_quickstart'
ORDER BY run_ts_utc DESC
LIMIT 10;
```

```sql
SELECT target_table, rule_name, status, failed_count
FROM main.ops.ctrl_ingestion_quality
WHERE target_table = 'main.bronze.b_orders_quickstart'
ORDER BY quality_ts_utc DESC;
```

## 5. Próximo passo

Depois que o fluxo mínimo passar, evolua para contrato YAML:

```yaml
source:
  type: connector
  connector: table
  table: main.raw.orders_quickstart
  read:
    source_complete: true

catalog: main
layer: bronze
target_table: b_orders_quickstart
mode: scd0_append
schema_policy: additive_only
ctrl_schema: ops

quality_rules:
  not_null: [order_id]
  unique_key: [order_id]
```

Valide antes de executar:

```bash
contractforge validate contracts/bronze/b_orders_quickstart.ingestion.yaml
```

