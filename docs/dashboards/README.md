# Dashboard Operacional das Ctrl Tables

Este pacote documenta um dashboard Databricks SQL para operar pipelines ContractForge em produção ou homologação. Ele não é um export JSON do Lakeview de propósito: o formato interno do dashboard pode mudar entre versões do Databricks. O repositório entrega o desenho visual, as queries e a hierarquia; a publicação final fica no workspace.

## Arquivos

- `control_tables_dashboard.sql`: consultas nomeadas para cards, gráficos, tabelas de drill-down e visões de qualidade.
- `control_tables_dashboard_blueprint.yaml`: organização recomendada de páginas, filtros, widgets e tipos de visualização.

## Objetivo Visual

O dashboard deve funcionar como um centro operacional, não como uma lista de queries. A primeira tela responde rapidamente:

- O ambiente está saudável agora?
- Quais targets falharam ou estão com SLA vencido?
- Qual volume foi processado e qual parte foi quarantinada?
- Onde está o gargalo: leitura, qualidade, escrita, lineage ou estado?
- Quais conectores/runtimes estão causando mais falhas?

## Parâmetros

Antes de criar as queries, substitua os placeholders:

| Placeholder | Exemplo | Descrição |
|-------------|---------|-----------|
| `{{catalog}}` | `main` | Catálogo onde ficam as ctrl tables |
| `{{ctrl_schema}}` | `ops` | Schema das ctrl tables |
| `{{lookback_days}}` | `7` | Janela padrão para gráficos históricos |

O Databricks SQL não parametriza identificadores como catálogo/schema em query parameters nativos. Por isso, substitua esses valores no SQL antes de salvar as queries.

## Estrutura Recomendada

Crie um dashboard chamado **ContractForge Operations Command Center** com as páginas abaixo.

| Página | Propósito | Visualizações principais |
|--------|-----------|--------------------------|
| Overview | Estado executivo do ambiente | KPI cards, trend de status, falhas recentes |
| Reliability | Saúde por target e SLA | matriz target/status, freshness, runbooks |
| Performance | Custo operacional indireto | duração, throughput, gargalos por etapa |
| Quality | Governança de qualidade | regras, severidade, quarentena, effective rows |
| Streaming | Auto Loader e foreachBatch | streams, microbatches, reconciliação filho/pai |
| Connectors & Governance | Adoção e cobertura operacional | conectores, runtimes, annotations/operations |

## Filtros Globais

Configure filtros no dashboard, quando possível:

- `run_date`
- `layer`
- `status`
- `target_table`
- `source_connector`
- `source_provider`
- `runtime_type`
- `criticality`

Para páginas executivas, mantenha `lookback_days` em 7 ou 14. Para troubleshooting, use 30 ou 90.

## Padrão Visual

Use cores consistentes:

| Status | Cor sugerida |
|--------|--------------|
| `SUCCESS` | Verde |
| `FAILED` | Vermelho |
| `SKIPPED` | Cinza |
| `DRY_RUN` | Azul |
| `WARN` / `WARNING` | Amarelo |
| `BREACHED` | Vermelho |
| `NO_SUCCESS` | Laranja |

Evite páginas com muitos gráficos pequenos. O blueprint prioriza poucos gráficos grandes, com tabelas de drill-down abaixo.

## Publicação no Databricks SQL

1. Abra `control_tables_dashboard.sql`.
2. Substitua `{{catalog}}`, `{{ctrl_schema}}` e `{{lookback_days}}`.
3. Crie uma query por bloco nomeado.
4. Monte as páginas conforme `control_tables_dashboard_blueprint.yaml`.
5. Valide com um período curto primeiro, por exemplo 7 dias.
6. Compartilhe o dashboard com os grupos operacionais que têm permissão de leitura nas ctrl tables.

## Permissões

O dashboard precisa de `SELECT` nas ctrl tables usadas:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_operations`

Se governance estiver habilitado, também é útil liberar:

- `ctrl_ingestion_annotations`
- `ctrl_ingestion_access`
- `ctrl_ingestion_schema_changes`

## O Que Não Fazer

- Não dê acesso amplo a `ctrl_ingestion_quarantine` sem revisar a política de dados sensíveis. A coluna `record_payload` pode conter dados rejeitados da origem.
- Não use `explain_mode=True` em produção contínua só para alimentar dashboard. `ctrl_ingestion_explain` é diagnóstico, não métrica operacional.
- Não trate `FAILED=0` como saúde suficiente. Verifique também SLA, quarentena e queda de volume.
