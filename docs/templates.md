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

## Templates Built-in

| Template | Uso |
|----------|-----|
| `bronze_rest_api_incremental` | API REST paginada com watermark e secrets. |
| `bronze_autoloader_json` | Auto Loader JSON em modo `available_now`. |
| `silver_jdbc_scd1_upsert` | JDBC incremental com SCD1, quality e access validate-only. |
| `silver_snapshot_soft_delete` | Snapshot completo com soft delete de ausentes. |
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
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"

target:
  catalog: main
  schema: sales_curated
  table: s_orders
```

## Ajustes Recomendados Depois de Gerar

- Troque `target.schema` e `target.table` para o padrão físico do projeto.
- Troque owners, grupos e runbook no arquivo `.operations.yaml`.
- Troque grants no `.access.yaml`.
- Substitua URLs e nomes de secrets.
- Rode `contractforge validate-bundle` e `contractforge governance-preview`.
