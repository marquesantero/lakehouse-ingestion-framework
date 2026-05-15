# Matriz de Compatibilidade de Conectores

Esta matriz descreve o contrato suportado pela lib. Drivers, credenciais, external locations e bibliotecas Spark externas continuam responsabilidade do runtime.

| Conector | Runtime esperado | Dependência externa | Local Spark | Databricks classic | Databricks serverless | Observações |
|----------|------------------|---------------------|-------------|--------------------|-----------------------|-------------|
| `table`, `delta_table`, `view` | Spark catalog | Nenhuma além do Spark/Delta | Sim | Sim | Sim | Depende de permissões no catálogo/schema/tabela. |
| `sql` | Spark SQL | Nenhuma além do Spark | Sim | Sim | Sim | Use para queries rastreáveis e versionadas; evite SQL muito grande no YAML. |
| `parquet`, `json`, `jsonl`, `ndjson`, `csv`, `orc`, `text`, `avro`, `xml` | Spark file reader | Conectores Hadoop/Spark do runtime | Sim | Sim | Sim | Path e credenciais precisam estar acessíveis ao Spark; `jsonl/ndjson` usam reader `json`; `xml` depende do suporte do runtime. |
| `http_file` | Driver Python | Biblioteca padrão `urllib` | Sim | Sim | Sim | Baixa HTTP(S) no driver e cria DataFrame; use `format=csv|json|jsonl|ndjson|text`. |
| `http_csv`, `http_json`, `http_text` | Driver Python | Biblioteca padrão `urllib` | Sim | Sim | Sim | Aliases de `http_file`; úteis quando Spark não consegue ler `https://` como filesystem. |
| `delta` | Spark Delta reader | Delta Lake | Sim com extra `spark` | Sim | Sim | Por path; para tabela registrada prefira `delta_table`/`table`. |
| `object_storage`, `blob` | Spark file reader | Credencial cloud configurada | Parcial | Sim | Sim | Use `provider=adls|azure_blob|s3|gcs`; para Azure Blob, SAS pode ser declarado em `auth.sas_token`. |
| `s3` | Spark file reader | Acesso S3 no runtime | Parcial | Sim | Sim | Alias de object storage com provider inferido. |
| `adls`, `azure_blob` | Spark file reader | Acesso Azure Storage no runtime/Unity Catalog ou SAS em runtime que permita config Hadoop | Parcial | Sim | Sim, via External Location/Volume ou rede liberada | `azure_blob` aceita `account_url`, `container` e `auth.sas_token`; se serverless bloquear `fs.azure.sas...`, a lib falha rápido com orientação operacional. |
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
- Para arquivo público HTTP(S) pequeno/médio, prefira `http_file` em vez de `spark.read` direto em `https://`, principalmente em serverless.
- Para APIs REST grandes, descarregue primeiro em landing files e use `autoloader`.
- Para `snapshot_soft_delete`, declare `source.read.source_complete=true` apenas quando a fonte representar o estado completo.
- Para JDBC em tabelas grandes, configure `partition_column`, `lower_bound`, `upper_bound`, `num_partitions` e `fetchsize`.
- Para Snowflake/BigQuery, valide o conector Spark no runtime antes de usar o contrato em produção.
- Para conectores que usam credenciais, use `{{ secret:scope/key }}` e valide que `contractforge validate`/`connectors doctor` não exibem segredo literal.
- Para Azure Blob com SAS, salve apenas o SAS token no secret scope e declare `account_url`, `container` e `path` separadamente no contrato.
- Para Azure Blob em Databricks serverless, prefira Unity Catalog External Location/Volume e paths `abfss://...` ou `/Volumes/...`; SAS direto via `fs.azure.sas...` pode ser bloqueado por Spark Connect.
- Se a origem é um arquivo HTTP(S) explícito e pequeno/médio, use `http_file`. Não trate `azure_blob` como downloader REST implícito.

## Exemplos de validação

```bash
contractforge connectors list
contractforge connectors show s3 postgres snowflake bigquery rest_api http_file
contractforge connectors doctor s3 postgres snowflake bigquery rest_api http_file
contractforge validate contracts/bronze/b_orders.ingestion.yaml
```

`connectors doctor` não abre conexão, não cria SparkSession e não valida credenciais. Ele mostra requisitos estáticos por conector, como driver JDBC, connector Spark externo, Auto Loader ou configuração cloud no runtime. Use esse comando em PRs e notebooks de diagnóstico antes de executar ingestões reais.

## Exemplo HTTP File CSV

Use `http_file` quando a origem é um arquivo publicado por HTTP(S), mas o runtime Spark não implementa leitura direta de `https://` como filesystem. O conector baixa o arquivo com Python e materializa o DataFrame no Spark, mantendo secrets e opções redigidos nas ctrl tables.

```yaml
source:
  type: connector
  connector: http_file
  path: https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv
  format: csv
  options:
    header: true
    nullValue: ""
  read:
    source_complete: true
  limits:
    timeout_seconds: 60
    retry_attempts: 3
    retry_backoff_seconds: 2

target:
  catalog: workspace
  schema: cf_examples_bronze
  table: b_covid_brazil_states

layer: bronze
mode: scd0_overwrite
source_system: covid19br_github
```

Aliases equivalentes:

```yaml
source:
  type: connector
  connector: http_csv
  path: https://example.com/data.csv
  options:
    header: true
```

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

## REST API com Payload JSON Complexo

Para JSON aninhado, arrays de structs ou payloads com schema variável, prefira `response.mode: raw`.
Nesse modo o conector apenas baixa uma linha por página com o JSON bruto em `raw_response`; a estruturação fica no `shape.parse_json`, com schema Spark DDL explícito.

```yaml
source:
  type: connector
  connector: rest_api
  name: nasa_eonet_events
  request:
    url: https://eonet.gsfc.nasa.gov/api/v3/events
    params:
      status: open
      limit: "50"
    headers:
      Accept: application/json
  response:
    mode: raw
    raw_column: raw_response
  limits:
    timeout_seconds: 60
    retry_attempts: 3
    max_page_bytes: 10485760
    max_total_bytes: 52428800

shape:
  parse_json:
    - column: raw_response
      alias: payload
      schema: >
        STRUCT<
          events: ARRAY<STRUCT<
            id: STRING,
            title: STRING,
            categories: ARRAY<STRUCT<id: STRING, title: STRING>>,
            geometry: ARRAY<STRUCT<date: STRING, type: STRING, coordinates: ARRAY<DOUBLE>>>
          >>
        >
```

Use `response.mode: records` quando a API retorna uma lista simples e estável em `records_path`. Use `response.mode: raw` quando a resposta precisa ser tratada por `shape`. Para payloads grandes ou replay recorrente, faça landing em storage e processe com Auto Loader.

