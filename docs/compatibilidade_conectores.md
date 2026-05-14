# Matriz de Compatibilidade de Conectores

Esta matriz descreve o contrato suportado pela lib. Drivers, credenciais, external locations e bibliotecas Spark externas continuam responsabilidade do runtime.

| Conector | Runtime esperado | Dependência externa | Local Spark | Databricks classic | Databricks serverless | Observações |
|----------|------------------|---------------------|-------------|--------------------|-----------------------|-------------|
| `table`, `delta_table`, `view` | Spark catalog | Nenhuma além do Spark/Delta | Sim | Sim | Sim | Depende de permissões no catálogo/schema/tabela. |
| `sql` | Spark SQL | Nenhuma além do Spark | Sim | Sim | Sim | Use para queries rastreáveis e versionadas; evite SQL muito grande no YAML. |
| `parquet`, `json`, `csv`, `orc`, `text` | Spark file reader | Conectores Hadoop do runtime | Sim | Sim | Sim | Path e credenciais precisam estar acessíveis ao Spark. |
| `delta` | Spark Delta reader | Delta Lake | Sim com extra `spark` | Sim | Sim | Por path; para tabela registrada prefira `delta_table`/`table`. |
| `object_storage`, `blob` | Spark file reader | Credencial cloud configurada | Parcial | Sim | Sim | Use `provider=adls|azure_blob|s3|gcs`. |
| `s3` | Spark file reader | Acesso S3 no runtime | Parcial | Sim | Sim | Alias de object storage com provider inferido. |
| `adls`, `azure_blob` | Spark file reader | Acesso Azure Storage no runtime | Parcial | Sim | Sim | Em Databricks, prefira UC external locations/Volumes quando possível. |
| `gcs` | Spark file reader | Acesso GCS no runtime | Parcial | Sim | Sim | Requer configuração GCS no cluster/serverless. |
| `jdbc` | Spark JDBC | Driver JDBC | Sim | Sim | Sim, se driver/runtime suportar | Exige `options.url` e `dbtable` ou `query`. |
| `postgres`, `postgresql` | Spark JDBC | Driver PostgreSQL | Sim | Sim | Sim, se driver disponível | Alias de `jdbc`; melhora clareza e observabilidade. |
| `sqlserver` | Spark JDBC | Driver Microsoft SQL Server | Sim | Sim | Sim, se driver disponível | Use `fetchsize` e particionamento em tabelas grandes. |
| `mysql` | Spark JDBC | Driver MySQL/MariaDB | Sim | Sim | Sim, se driver disponível | Alias de `jdbc`. |
| `oracle` | Spark JDBC | Driver Oracle | Sim | Sim | Sim, se driver disponível | Driver costuma exigir distribuição/licença controlada. |
| `rest_api` | Driver Python | Biblioteca padrão `urllib` | Sim | Sim | Sim | Adequado para APIs paginadas de volume controlado. |
| `snowflake` | Spark connector | Spark Snowflake connector | Sim, se instalado | Sim, se instalado | Sim, se suportado pelo runtime | Delegado a `spark.read.format("snowflake")`. |
| `bigquery` | Spark connector | Spark BigQuery connector | Sim, se instalado | Sim, se instalado | Sim, se suportado pelo runtime | Delegado a `spark.read.format("bigquery")`. |
| `autoloader` | Databricks Auto Loader | Databricks Runtime | Não | Sim | Sim | Apenas `available_now`; streaming contínuo fica fora do escopo. |

## Regras práticas

- Para arquivos recorrentes ou alto volume, prefira `autoloader` em Databricks.
- Para APIs REST grandes, descarregue primeiro em landing files e use `autoloader`.
- Para `snapshot_soft_delete`, declare `source.read.source_complete=true` apenas quando a fonte representar o estado completo.
- Para JDBC em tabelas grandes, configure `partition_column`, `lower_bound`, `upper_bound`, `num_partitions` e `fetchsize`.
- Para Snowflake/BigQuery, valide o conector Spark no runtime antes de usar o contrato em produção.
- Para conectores que usam credenciais, use `{{ secret:scope/key }}` e valide que `contractforge validate`/`connectors doctor` não exibem segredo literal.

## Exemplos de validação

```bash
contractforge connectors list
contractforge connectors show s3 postgres snowflake bigquery rest_api
contractforge connectors doctor s3 postgres snowflake bigquery rest_api
contractforge validate contracts/bronze/b_orders.ingestion.yaml
```

`connectors doctor` não abre conexão, não cria SparkSession e não valida credenciais. Ele mostra requisitos estáticos por conector, como driver JDBC, connector Spark externo, Auto Loader ou configuração cloud no runtime. Use esse comando em PRs e notebooks de diagnóstico antes de executar ingestões reais.

## Exemplo JDBC Incremental

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    dbtable: public.orders
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
  incremental:
    watermark_column: updated_at
  read:
    fetchsize: 10000
    partition_column: id
    lower_bound: 1
    upper_bound: 10000000
    num_partitions: 16

target:
  catalog: main
  schema: sales_curated
  table: s_orders

layer: silver
mode: scd1_upsert
merge_keys: order_id
watermark_columns: updated_at
```

## Exemplo REST API Incremental

```yaml
source:
  type: connector
  connector: rest_api
  name: orders_api
  request:
    url: https://api.example.com/orders
    params:
      status: open
  auth:
    type: bearer_token
    token: "{{ secret:orders_api/token }}"
  pagination:
    type: cursor
    cursor_param: cursor
    next_cursor_path: $.next
  response:
    records_path: $.data
  incremental:
    watermark_param: updated_after
    watermark_header: X-Watermark
  limits:
    max_pages: 100
    timeout_seconds: 60
    retry_attempts: 3

target:
  catalog: main
  schema: bronze
  table: b_orders_api

layer: bronze
mode: scd0_append
watermark_columns: updated_at
```

