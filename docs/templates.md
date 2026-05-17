# Templates de Contratos

Templates são exemplos executáveis de contratos completos. Eles não substituem presets:

- `preset` define defaults reutilizáveis dentro de um contrato.
- `template` gera arquivos YAML de partida para um cenário real.

Use templates para acelerar onboarding e padronizar projetos novos.

## Listar Templates

```bash
contractforge templates list
```

## Ver Um Template

```bash
contractforge templates show silver_jdbc_scd1_upsert
contractforge templates show silver_jdbc_scd1_upsert --metadata-only
```

## Gerar Um Bundle YAML

```bash
contractforge templates write silver_jdbc_scd1_upsert \
  --output contracts/silver/s_orders
```

Esse comando gera arquivos split quando o template possui governança:

```text
contracts/silver/s_orders.ingestion.yaml
contracts/silver/s_orders.annotations.yaml
contracts/silver/s_orders.operations.yaml
contracts/silver/s_orders.access.yaml
```

Depois valide:

```bash
contractforge validate-bundle contracts/silver/s_orders
contractforge governance-preview contracts/silver/s_orders
```

## Wizard de Templates

Use `templates wizard` para recomendar templates por cenário antes de gerar arquivos:

```bash
contractforge templates wizard --layer silver --source jdbc --mode scd1_upsert
contractforge templates wizard --layer bronze --source s3 --pattern partitioned
contractforge templates wizard --layer bronze --source http_file --pattern csv
contractforge templates wizard --layer silver --source jdbc --pattern rds_iam
contractforge templates wizard --layer silver --pattern hash_diff --limit 1
```

Para gravar o melhor template recomendado:

```bash
contractforge templates wizard \
  --layer bronze \
  --source s3 \
  --output contracts/bronze/b_orders_files
```

Se quiser gravar um template específico dentro do mesmo fluxo:

```bash
contractforge templates wizard \
  --layer silver \
  --pattern hash_diff \
  --name silver_scd1_hash_diff \
  --output contracts/silver/s_products_hash_diff
```

O wizard é determinístico: ele não usa IA nem abre conexão com Databricks. O retorno JSON inclui `score`, `matched` e os metadados de cada template recomendado.

## Templates Built-in

| Template | Uso |
|----------|-----|
| `bronze_rest_api_incremental` | API REST paginada com watermark e secrets. |
| `bronze_http_file_csv_snapshot` | CSV público/autenticado via HTTP(S), com schema explícito e overwrite. |
| `bronze_autoloader_json` | Auto Loader JSON em modo `available_now`. |
| `bronze_autoloader_available_now_json` | Auto Loader `available_now` com checkpoint externo e controle de microbatches. |
| `bronze_blob_partitioned_files` | CSV/Parquet particionado em S3/Blob/ADLS/GCS com schema explícito e filtro opcional. |
| `bronze_object_storage_nested_json_shape` | JSON aninhado em object storage com `transform.shape.columns`. |
| `bronze_object_storage_small_files` | Muitas dezenas/centenas de arquivos pequenos com glob, regex e schema explícito. |
| `silver_jdbc_scd1_upsert` | JDBC incremental com SCD1, quality e access validate-only. |
| `silver_jdbc_rds_iam_hash_diff` | Amazon RDS/Aurora IAM auth com JDBC incremental e hash diff. |
| `silver_raw_json_payload_shape` | Coluna JSON string normalizada com `transform.shape.parse_json`. |
| `silver_parallel_arrays_shape` | Arrays paralelos de APIs normalizados com `zip_arrays` + `explode_outer`. |
| `silver_snapshot_soft_delete` | Snapshot completo com soft delete de ausentes. |
| `silver_scd1_hash_diff` | Hash diff append-only para manter versões alteradas. |
| `silver_scd2_history` | Histórico SCD2 para dimensões mutáveis. |
| `gold_full_refresh_kpi` | Gold full refresh para tabela agregada/KPI. |

## Exemplo: API REST Para Bronze

```bash
contractforge templates write bronze_rest_api_incremental \
  --output contracts/bronze/b_orders_api
```

O template gerado usa:

```yaml
source:
  type: connector
  connector: rest_api
  auth:
    type: bearer_token
    token: "{{ secret:orders_api/token }}"
  pagination:
    type: cursor
  incremental:
    watermark_param: updated_after
    watermark_header: X-Watermark
```

## Exemplo: JDBC Para Silver

```bash
contractforge templates write silver_jdbc_scd1_upsert \
  --output contracts/silver/s_orders
```

O template gerado combina:

```yaml
preset:
  - silver_incremental_watermark_upsert
  - quality_quarantine
  - delta_optimized_writes

source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    dbtable: public.orders
  auth:
    type: basic
    username: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"

target:
  catalog: main
  schema: sales_curated
  table: s_orders
```

## Exemplo: Shape Para JSON Aninhado

```bash
contractforge templates write bronze_object_storage_nested_json_shape \
  --output contracts/bronze/b_earthquake_events
```

O template gerado usa `transform.shape` para projetar campos aninhados e expressões, sem PySpark manual:

```yaml
transform:
  shape:
    columns:
      id: event_id
      properties.mag:
        alias: magnitude
        cast: DOUBLE
      properties.time:
        alias: event_time
        expression: "CAST(properties.time / 1000 AS TIMESTAMP)"
      longitude_expr:
        alias: longitude
        expression: "element_at(geometry.coordinates, 1)"
```

## Exemplo: RDS/Aurora IAM + Hash Diff

```bash
contractforge templates write silver_jdbc_rds_iam_hash_diff \
  --output contracts/silver/s_orders_hash_diff
```

Esse template demonstra:

- `auth.type: rds_iam`
- `credential_provider: default_chain`
- particionamento JDBC com `partition_column`, bounds e `num_partitions`
- `transform.deduplicate`
- `mode: scd1_hash_diff`

## Ajustes Recomendados Depois de Gerar

- Troque `target.schema` e `target.table` para o padrão físico do projeto.
- Troque owners, grupos e runbook no arquivo `.operations.yaml`.
- Troque grants no `.access.yaml`.
- Substitua URLs e nomes de secrets.
- Rode `contractforge validate-bundle` e `contractforge governance-preview`.
