# Operação e Manutenção

Este guia cobre rotinas operacionais que não fazem parte da ingestão em si, mas mantêm o ambiente saudável.

## Retenção das Ctrl Tables

As tabelas `ctrl_*` são evidência operacional. Elas devem ser preservadas por tempo suficiente para auditoria, suporte e troubleshooting, mas não devem crescer indefinidamente.

Recomendação inicial:

| Ambiente | Retenção sugerida | Observação |
|----------|-------------------|------------|
| Desenvolvimento | 15 a 30 dias | Evita acúmulo local. |
| Homologação | 30 a 90 dias | Útil para regressões e validação de releases. |
| Produção | 180 a 400 dias | Ajuste conforme auditoria, LGPD e políticas internas. |

O estado corrente (`ctrl_ingestion_state`) e metadados de versão (`ctrl_ingestion_metadata`) não entram na limpeza histórica. Eles representam o estado operacional atual.

## Preview

Por padrão, o comando apenas imprime o plano SQL:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180
```

Para limpar apenas algumas tabelas:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 90 \
  --target runs \
  --target errors \
  --target quarantine
```

## Aplicação

Use `--apply` somente em job operacional controlado:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180 \
  --apply
```

Para executar `VACUUM` após os `DELETE`s:

```bash
contractforge maintenance ctrl-retention \
  --catalog main \
  --ctrl-schema ops \
  --retention-days 180 \
  --vacuum \
  --vacuum-retention-hours 168 \
  --apply
```

## Contrato Operacional Recomendado

Em projetos com contratos separados, registre a criticidade da própria tabela ingerida no `*.operations.yaml`:

```yaml
target:
  catalog: main
  schema: sales_curated
  table: s_orders

ownership:
  business_owner: sales-ops
  technical_owner: data-platform
  support_group: data-platform

operations:
  criticality: high
  expected_frequency: daily
  freshness_sla_minutes: 180
  alert_on_failure: true
  alert_on_quality_fail: true
  runbook_url: https://wiki.example.com/runbooks/s_orders
  tags:
    maintenance_window: "02:00-04:00 UTC"
```

O ContractForge não envia alertas diretamente. Ele registra dados suficientes em `ctrl_ingestion_runs`, `ctrl_ingestion_errors`, `ctrl_ingestion_quality`, `ctrl_ingestion_streams` e `ctrl_ingestion_operations` para dashboards e ferramentas externas.

Para um dashboard operacional completo no Databricks SQL, use o pacote em [`docs/dashboards`](dashboards/README.md). Ele traz blueprint de páginas, filtros, widgets e queries para visão executiva, confiabilidade, performance, qualidade, streaming, conectores e governança.

## Tabelas Históricas Limpas

O comando atua sobre:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_locks`
- `ctrl_ingestion_explain`
- `ctrl_ingestion_lineage`
- `ctrl_ingestion_schema_changes`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_annotations`
- `ctrl_ingestion_operations`
- `ctrl_ingestion_access`

Não limpa:

- `ctrl_ingestion_state`
- `ctrl_ingestion_metadata`

## Práticas

- Agende a limpeza fora da janela principal de ingestão.
- Rode primeiro sem `--apply` e revise o SQL gerado.
- Use `VACUUM` somente se a política de retenção Delta do ambiente permitir.
- Restrinja permissões de execução desse comando ao time de plataforma.
