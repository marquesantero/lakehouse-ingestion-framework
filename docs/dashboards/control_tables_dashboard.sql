-- ContractForge Operations Command Center
--
-- Substitua antes de criar as queries:
--   {{catalog}}       -> catálogo das ctrl tables, ex.: main
--   {{ctrl_schema}}   -> schema das ctrl tables, ex.: ops
--   {{lookback_days}} -> janela em dias, ex.: 7
--
-- Recomendações:
--   - Crie uma query no Databricks SQL para cada bloco qNN.
--   - Use o blueprint YAML para organizar páginas e visualizações.
--   - Não execute este arquivo inteiro de uma vez se sua ferramenta não aceitar
--     múltiplos SELECTs independentes.

-- q01_executive_kpis
-- Visualização: Counter cards.
-- Uso: topo da página Overview.
WITH base AS (
  SELECT *
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
  WHERE run_date >= date_sub(current_date(), {{lookback_days}})
)
SELECT
  count(*) AS total_runs,
  sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
  sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
  round(100.0 * sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / nullif(count(*), 0), 2) AS success_rate_pct,
  count(DISTINCT target_table) AS active_targets,
  sum(coalesce(rows_read, 0)) AS rows_read,
  sum(coalesce(rows_written, 0)) AS rows_written,
  sum(coalesce(rows_quarantined, 0)) AS rows_quarantined,
  round(avg(duration_seconds), 2) AS avg_duration_seconds
FROM base;

-- q02_status_trend
-- Visualização: stacked bar ou stacked area por run_date.
-- Uso: tendência diária de saúde.
SELECT
  run_date,
  layer,
  status,
  count(*) AS runs,
  sum(coalesce(rows_read, 0)) AS rows_read,
  sum(coalesce(rows_written, 0)) AS rows_written,
  sum(coalesce(rows_quarantined, 0)) AS rows_quarantined,
  round(avg(duration_seconds), 2) AS avg_duration_seconds
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY run_date, layer, status
ORDER BY run_date, layer, status;

-- q03_latest_target_health
-- Visualização: tabela com formatação condicional em status, quality_status e minutes_since_finish.
-- Uso: estado corrente por target.
WITH ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY target_table
      ORDER BY run_ts_utc DESC, finished_at_utc DESC
    ) AS rn
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
  WHERE run_date >= date_sub(current_date(), {{lookback_days}})
)
SELECT
  target_table,
  layer,
  mode,
  status,
  quality_status,
  source_connector,
  source_provider,
  source_format,
  rows_read,
  rows_written,
  rows_quarantined,
  round(coalesce(rows_quarantined, 0) / nullif(coalesce(rows_read, 0), 0), 4) AS quarantine_ratio,
  duration_seconds,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(finished_at_utc)) / 60, 1) AS minutes_since_finish,
  finished_at_utc,
  runtime_type,
  framework_version,
  error_message
FROM ranked
WHERE rn = 1
ORDER BY
  CASE status WHEN 'FAILED' THEN 0 WHEN 'SUCCESS' THEN 1 WHEN 'SKIPPED' THEN 2 ELSE 3 END,
  minutes_since_finish DESC NULLS LAST;

-- q04_recent_failures
-- Visualização: tabela com drill-down.
-- Uso: primeira investigação de falhas.
SELECT
  e.error_ts_utc,
  e.target_table,
  r.layer,
  e.mode,
  e.error_type,
  e.error_message,
  r.source_connector,
  r.source_provider,
  r.source_format,
  r.runtime_type,
  r.framework_version,
  r.run_id
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_errors e
LEFT JOIN {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs r
  ON e.run_id = r.run_id
WHERE e.error_date >= date_sub(current_date(), {{lookback_days}})
ORDER BY e.error_ts_utc DESC;

-- q05_target_reliability
-- Visualização: heatmap/tabela por target.
-- Uso: identificar targets instáveis.
SELECT
  target_table,
  layer,
  mode,
  count(*) AS runs,
  sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
  sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
  round(100.0 * sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / nullif(count(*), 0), 2) AS success_rate_pct,
  round(avg(duration_seconds), 2) AS avg_duration_seconds,
  max(finished_at_utc) AS last_finished_at_utc,
  max(error_message) FILTER (WHERE status = 'FAILED') AS sample_error_message
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY target_table, layer, mode
ORDER BY failed_runs DESC, success_rate_pct ASC, target_table;

-- q06_sla_freshness
-- Visualização: tabela com status colorido.
-- Uso: targets sem sucesso recente ou com SLA vencido.
WITH latest_success AS (
  SELECT
    target_table,
    max(finished_at_utc) AS last_success_at_utc
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
  WHERE status = 'SUCCESS'
  GROUP BY target_table
),
latest_operations AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY target_table
      ORDER BY recorded_at_utc DESC
    ) AS rn
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_operations
)
SELECT
  o.target_table,
  o.criticality,
  o.expected_frequency,
  o.freshness_sla_minutes,
  s.last_success_at_utc,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(s.last_success_at_utc)) / 60, 1) AS minutes_since_success,
  CASE
    WHEN s.last_success_at_utc IS NULL THEN 'NO_SUCCESS'
    WHEN o.freshness_sla_minutes IS NULL THEN 'NO_SLA'
    WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(s.last_success_at_utc)) / 60 > o.freshness_sla_minutes THEN 'BREACHED'
    ELSE 'OK'
  END AS freshness_status,
  o.alert_on_failure,
  o.alert_on_quality_fail,
  o.runbook_url,
  o.ownership_json,
  o.groups_json,
  o.tags_json
FROM latest_operations o
LEFT JOIN latest_success s
  ON o.target_table = s.target_table
WHERE o.rn = 1
ORDER BY
  CASE freshness_status WHEN 'BREACHED' THEN 0 WHEN 'NO_SUCCESS' THEN 1 WHEN 'OK' THEN 2 ELSE 3 END,
  CASE o.criticality WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,
  o.target_table;

-- q07_failure_taxonomy
-- Visualização: horizontal bar por error_type.
-- Uso: identificar classes de erro dominantes.
SELECT
  coalesce(e.error_type, 'unknown') AS error_type,
  r.source_connector,
  r.runtime_type,
  count(*) AS failures,
  count(DISTINCT e.target_table) AS affected_targets,
  max(e.error_ts_utc) AS last_error_ts_utc,
  max(e.error_message) AS sample_error_message
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_errors e
LEFT JOIN {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs r
  ON e.run_id = r.run_id
WHERE e.error_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY coalesce(e.error_type, 'unknown'), r.source_connector, r.runtime_type
ORDER BY failures DESC, affected_targets DESC;

-- q08_error_drilldown
-- Visualização: tabela ampla. Exibir stack_trace somente em drill-down, não em tela executiva.
-- Uso: investigação técnica.
SELECT
  e.error_ts_utc,
  e.run_id,
  e.target_table,
  r.layer,
  r.mode,
  r.source_connector,
  r.source_path,
  r.runtime_type,
  e.error_type,
  e.error_message,
  e.stack_trace
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_errors e
LEFT JOIN {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs r
  ON e.run_id = r.run_id
WHERE e.error_date >= date_sub(current_date(), {{lookback_days}})
ORDER BY e.error_ts_utc DESC;

-- q09_duration_percentiles
-- Visualização: grouped bar por mode/layer.
-- Uso: baseline de latência por modo.
SELECT
  layer,
  mode,
  count(*) AS successful_runs,
  round(avg(duration_seconds), 2) AS avg_duration_seconds,
  round(percentile_approx(duration_seconds, 0.50), 2) AS p50_duration_seconds,
  round(percentile_approx(duration_seconds, 0.95), 2) AS p95_duration_seconds,
  round(max(duration_seconds), 2) AS max_duration_seconds
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
  AND status = 'SUCCESS'
  AND duration_seconds IS NOT NULL
GROUP BY layer, mode
ORDER BY p95_duration_seconds DESC;

-- q10_stage_duration_breakdown
-- Visualização: stacked bar por target/run.
-- Uso: descobrir gargalo por etapa.
WITH stages AS (
  SELECT
    run_id,
    run_date,
    target_table,
    layer,
    mode,
    status,
    duration_seconds,
    from_json(stage_durations_json, 'map<string,double>') AS stage_map
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
  WHERE run_date >= date_sub(current_date(), {{lookback_days}})
    AND status = 'SUCCESS'
),
exploded AS (
  SELECT
    run_id,
    run_date,
    target_table,
    layer,
    mode,
    status,
    duration_seconds,
    stage.key AS stage_name,
    stage.value AS stage_seconds
  FROM stages
  LATERAL VIEW explode(map_entries(stage_map)) exploded_stage AS stage
)
SELECT
  run_date,
  target_table,
  layer,
  mode,
  stage_name,
  round(sum(coalesce(stage_seconds, 0)), 3) AS stage_seconds,
  round(sum(coalesce(duration_seconds, 0)), 3) AS run_seconds
FROM exploded
GROUP BY run_date, target_table, layer, mode, stage_name
ORDER BY run_date DESC, target_table, stage_seconds DESC;

-- q11_throughput_by_target
-- Visualização: scatter ou tabela ordenada.
-- Uso: encontrar targets com baixa vazão.
SELECT
  target_table,
  layer,
  mode,
  count(*) AS runs,
  sum(coalesce(rows_read, 0)) AS rows_read,
  sum(coalesce(rows_written, 0)) AS rows_written,
  round(sum(coalesce(rows_written, 0)) / nullif(sum(duration_seconds), 0), 2) AS rows_written_per_second,
  round(avg(duration_seconds), 2) AS avg_duration_seconds,
  max(finished_at_utc) AS last_finished_at_utc
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
  AND status = 'SUCCESS'
GROUP BY target_table, layer, mode
ORDER BY rows_written_per_second ASC NULLS LAST, rows_written DESC;

-- q12_slowest_runs
-- Visualização: tabela.
-- Uso: drill-down de execuções caras/lentas.
SELECT
  run_ts_utc,
  target_table,
  layer,
  mode,
  source_connector,
  source_provider,
  source_format,
  rows_read,
  rows_written,
  duration_seconds,
  round(coalesce(rows_written, 0) / nullif(duration_seconds, 0), 2) AS rows_written_per_second,
  metrics_source,
  runtime_type,
  run_id
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
  AND status = 'SUCCESS'
ORDER BY duration_seconds DESC
LIMIT 50;

-- q13_quality_summary
-- Visualização: stacked bar por status/severity.
-- Uso: visão agregada de qualidade.
SELECT
  q.status,
  q.severity,
  count(*) AS rule_evaluations,
  sum(coalesce(q.failed_count, 0)) AS failed_count,
  count(DISTINCT q.target_table) AS affected_targets,
  max(q.checked_at_utc) AS last_checked_at_utc
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_quality q
JOIN {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs r
  ON q.run_id = r.run_id
WHERE r.run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY q.status, q.severity
ORDER BY failed_count DESC, rule_evaluations DESC;

-- q14_quality_rules_hotspots
-- Visualização: horizontal bar por rule_name.
-- Uso: regras que mais causam falhas.
SELECT
  q.target_table,
  q.rule_name,
  q.status,
  q.severity,
  count(*) AS occurrences,
  sum(coalesce(q.failed_count, 0)) AS failed_count,
  max(q.checked_at_utc) AS last_checked_at_utc,
  max(q.message) AS sample_message
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_quality q
JOIN {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs r
  ON q.run_id = r.run_id
WHERE r.run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY q.target_table, q.rule_name, q.status, q.severity
ORDER BY failed_count DESC, occurrences DESC;

-- q15_quarantine_hotspots
-- Visualização: bar/tabela.
-- Uso: tabelas e regras com mais dados isolados.
SELECT
  target_table,
  rule_name,
  count(*) AS quarantined_records,
  max(quarantined_at_utc) AS last_quarantined_at_utc
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_quarantine
WHERE quarantined_at_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
GROUP BY target_table, rule_name
ORDER BY quarantined_records DESC;

-- q16_effective_rows
-- Visualização: stacked bar por target_table.
-- Uso: comparar rows_written úteis vs quarentena.
SELECT
  target_table,
  layer,
  sum(coalesce(rows_read, 0)) AS rows_read,
  sum(coalesce(rows_written, 0)) AS rows_written,
  sum(coalesce(rows_quarantined, 0)) AS rows_quarantined,
  sum(coalesce(rows_read, 0) - coalesce(rows_quarantined, 0)) AS effective_rows,
  round(sum(coalesce(rows_quarantined, 0)) / nullif(sum(coalesce(rows_read, 0)), 0), 4) AS quarantine_ratio
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY target_table, layer
ORDER BY quarantine_ratio DESC NULLS LAST, rows_quarantined DESC;

-- q17_stream_kpis
-- Visualização: Counter cards.
-- Uso: topo da página Streaming.
SELECT
  count(*) AS stream_runs,
  sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_streams,
  sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_streams,
  sum(coalesce(batches_processed, 0)) AS batches_processed,
  sum(coalesce(total_rows_read, 0)) AS total_rows_read,
  sum(coalesce(total_rows_written, 0)) AS total_rows_written,
  sum(coalesce(total_rows_quarantined, 0)) AS total_rows_quarantined,
  round(avg(duration_seconds), 2) AS avg_stream_duration_seconds
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_streams
WHERE started_at_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS;

-- q18_stream_runs
-- Visualização: tabela.
-- Uso: execução externa do stream/Auto Loader.
SELECT
  stream_run_id,
  target_table,
  target_layer,
  source_type,
  source_path,
  trigger,
  status,
  batches_processed,
  total_rows_read,
  total_rows_written,
  total_rows_quarantined,
  duration_seconds,
  started_at_utc,
  ended_at_utc,
  checkpoint_location,
  runtime_type,
  error_message
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_streams
WHERE started_at_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
ORDER BY started_at_utc DESC;

-- q19_stream_child_reconciliation
-- Visualização: tabela com status de reconciliação.
-- Uso: validar se métricas agregadas batem com runs filhos.
WITH child_runs AS (
  SELECT
    parent_run_id AS stream_run_id,
    count(*) AS child_runs,
    sum(coalesce(rows_read, 0)) AS child_rows_read,
    sum(coalesce(rows_written, 0)) AS child_rows_written,
    sum(coalesce(rows_quarantined, 0)) AS child_rows_quarantined,
    sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_child_runs
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
  WHERE parent_run_id IS NOT NULL
    AND run_date >= date_sub(current_date(), {{lookback_days}})
  GROUP BY parent_run_id
)
SELECT
  s.stream_run_id,
  s.target_table,
  s.status,
  s.batches_processed,
  coalesce(c.child_runs, 0) AS child_runs,
  s.total_rows_read,
  coalesce(c.child_rows_read, 0) AS child_rows_read,
  s.total_rows_written,
  coalesce(c.child_rows_written, 0) AS child_rows_written,
  s.total_rows_quarantined,
  coalesce(c.child_rows_quarantined, 0) AS child_rows_quarantined,
  coalesce(c.failed_child_runs, 0) AS failed_child_runs,
  CASE
    WHEN s.batches_processed = coalesce(c.child_runs, 0)
     AND s.total_rows_written = coalesce(c.child_rows_written, 0)
     AND s.total_rows_read = coalesce(c.child_rows_read, 0)
    THEN 'OK'
    ELSE 'CHECK'
  END AS reconciliation_status
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_streams s
LEFT JOIN child_runs c
  ON s.stream_run_id = c.stream_run_id
WHERE s.started_at_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
ORDER BY s.started_at_utc DESC;

-- q20_connector_runtime_matrix
-- Visualização: grouped bar ou heatmap.
-- Uso: uso e taxa de falha por conector/runtime.
SELECT
  source_connector,
  source_provider,
  source_format,
  runtime_type,
  count(*) AS runs,
  sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
  round(100.0 * sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / nullif(count(*), 0), 2) AS failure_rate_pct,
  round(avg(duration_seconds), 2) AS avg_duration_seconds,
  max(run_ts_utc) AS last_run_ts_utc
FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_runs
WHERE run_date >= date_sub(current_date(), {{lookback_days}})
GROUP BY source_connector, source_provider, source_format, runtime_type
ORDER BY runs DESC, failure_rate_pct DESC;

-- q21_operations_coverage
-- Visualização: tabela com completeness_score.
-- Uso: descobrir targets sem dono, SLA ou runbook.
WITH latest_operations AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY target_table
      ORDER BY recorded_at_utc DESC
    ) AS rn
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_operations
)
SELECT
  target_table,
  criticality,
  expected_frequency,
  freshness_sla_minutes,
  get_json_object(owners_json, '$.business_owner') AS business_owner,
  get_json_object(owners_json, '$.technical_owner') AS technical_owner,
  get_json_object(groups_json, '$.support_group') AS support_group,
  runbook_url,
  (
    CASE WHEN criticality IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN expected_frequency IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN freshness_sla_minutes IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN get_json_object(owners_json, '$.business_owner') IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN get_json_object(owners_json, '$.technical_owner') IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN get_json_object(groups_json, '$.support_group') IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN runbook_url IS NOT NULL THEN 1 ELSE 0 END
  ) AS completeness_score,
  recorded_at_utc,
  status
FROM latest_operations
WHERE rn = 1
ORDER BY completeness_score ASC, target_table;

-- q22_governance_artifacts
-- Visualização: tabela.
-- Uso: acompanhar annotations/access/schema changes recentes.
WITH schema_changes AS (
  SELECT
    target_table,
    count(*) AS schema_change_events,
    max(change_ts_utc) AS last_schema_change_at
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_schema_changes
  WHERE change_ts_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
  GROUP BY target_table
),
annotations AS (
  SELECT
    target_table,
    count(*) AS annotation_events,
    sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_annotation_events,
    max(annotation_ts_utc) AS last_annotation_at
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_annotations
  WHERE annotation_ts_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
  GROUP BY target_table
),
access_events AS (
  SELECT
    target_table,
    count(*) AS access_events,
    sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_access_events,
    max(access_ts_utc) AS last_access_at
  FROM {{catalog}}.{{ctrl_schema}}.ctrl_ingestion_access
  WHERE access_ts_utc >= current_timestamp() - INTERVAL {{lookback_days}} DAYS
  GROUP BY target_table
)
SELECT
  coalesce(s.target_table, a.target_table, x.target_table) AS target_table,
  coalesce(s.schema_change_events, 0) AS schema_change_events,
  s.last_schema_change_at,
  coalesce(a.annotation_events, 0) AS annotation_events,
  coalesce(a.failed_annotation_events, 0) AS failed_annotation_events,
  a.last_annotation_at,
  coalesce(x.access_events, 0) AS access_events,
  coalesce(x.failed_access_events, 0) AS failed_access_events,
  x.last_access_at
FROM schema_changes s
FULL OUTER JOIN annotations a
  ON s.target_table = a.target_table
FULL OUTER JOIN access_events x
  ON coalesce(s.target_table, a.target_table) = x.target_table
ORDER BY
  failed_access_events DESC,
  failed_annotation_events DESC,
  schema_change_events DESC,
  target_table;
