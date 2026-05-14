# Guidelines de Performance

ContractForge padroniza padrões de ingestão, mas performance final depende do modo de escrita, tamanho da origem, particionamento e runtime Spark.

## Escolha do modo

| Cenário | Modo recomendado | Observação |
|---------|------------------|------------|
| Landing append-only | `scd0_append` | Mais barato; combine com idempotência quando reprocessar lotes. |
| Reprocessamento completo pequeno | `scd0_overwrite` | Simples, mas reescreve tudo. |
| Dimensão atual por chave | `scd1_upsert` | Exige `merge_keys`; cuide de chaves nulas. |
| Upsert com redução de merges inúteis | `scd1_hash_diff` | Use `dedup_order_expr` determinístico. |
| Histórico tipo 2 | `scd2_historical` | Mais custoso; considere particionamento e volume de mudanças. |
| Snapshot completo com soft delete | `snapshot_soft_delete` | Exige fonte completa, sem watermark/filter incremental. |

## Cache

- Use `use_cache=true` apenas quando o mesmo DataFrame for reutilizado por etapas caras.
- Evite cache em origens grandes quando o cluster tem memória limitada.
- Se houver OOM, primeiro desligue cache e reduza paralelismo/partições de leitura.

## JDBC

- Sempre particione leituras grandes por coluna numérica ou temporal estável.
- Evite `query` complexa sem índice do lado da origem.
- Use pushdown incremental (`source.incremental.watermark_column` ou `predicate`) para reduzir volume.
- Ajuste `fetchsize` conforme banco/driver.

## REST API

- Use `limits.max_pages`, `timeout_seconds`, `retry_attempts`, `retry_backoff_seconds` e `rate_limit_per_minute`.
- Não use REST direto para cargas massivas ou replay bruto. Grave raw files e processe com Auto Loader.
- Registre `response.records_path` para evitar DataFrames com payloads aninhados desnecessários.

## Delta layout

- Para novas tabelas Databricks, prefira `cluster_columns` quando fizer sentido para Liquid Clustering.
- Use `partition_column` apenas quando a cardinalidade e o padrão de filtro justificarem.
- `zorder_columns` só tem efeito quando `optimize_after_write=true` e o runtime suporta a operação.

## Observabilidade de custo

Monitore por tabela:

```sql
SELECT
  target_table,
  mode,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(rows_written) AS avg_rows_written,
  AVG(rows_written / NULLIF(duration_seconds, 0)) AS avg_rows_per_second
FROM main.ops.ctrl_ingestion_runs
WHERE status = 'SUCCESS'
GROUP BY target_table, mode
ORDER BY avg_duration_seconds DESC;
```

