# ContractForge — Documentação Oficial

**Versão:** 2.6.5 | **Licença:** MIT | **Python:** >= 3.10

Framework declarativo para ingestão de dados em Delta Lake no Databricks (ou PySpark + delta-spark standalone), com contratos por tabela, suporte à arquitetura Medallion e classificações lógicas customizadas, conectores declarativos, quality gates, watermarks tipados, 6 modos de escrita, snapshot com soft delete, evolução de schema, ingestão Autoloader `available_now`, explain mode e emissão de eventos OpenLineage.

---

## Índice

1. [Visão Geral e Conceitos](#1-visão-geral-e-conceitos)
2. [Instalação](#2-instalação)
3. [Quick Start](#3-quick-start)
4. [API Pública](#4-api-pública)
5. [Referência Completa de Parâmetros do IngestionPlan](#5-referência-completa-de-parâmetros-do-ingestionplan)
5C. [Fontes e Conectores Declarativos](#5c-fontes-e-conectores-declarativos)
5D. [Presets Declarativos](#5d-presets-declarativos)
5E. [Shape Declarativo para JSON, Structs e Arrays](#5e-shape-declarativo-para-json-structs-e-arrays)
6. [Modos de Escrita — Guia Detalhado](#6-modos-de-escrita--guia-detalhado)
7. [Quality Gates — Guia Completo](#7-quality-gates--guia-completo)
8. [Schema Policy — Evolução de Schema](#8-schema-policy--evolução-de-schema)
9. [Watermarks — Carga Incremental](#9-watermarks--carga-incremental)
10. [Estratégias de Merge (scd1_upsert)](#10-estratégias-de-merge-scd1_upsert)
11. [Locks, Idempotência, Retry e Concorrência](#11-locks-idempotência-retry-e-concorrência)
12. [Observabilidade — Tabelas de Controle](#12-observabilidade--tabelas-de-controle)
13. [OpenLineage e Explain Mode](#13-openlineage-e-explain-mode)
14. [Linhagem Operacional (parent/master)](#14-linhagem-operacional-parentmaster)
15. [Metadados de Contrato](#15-metadados-de-contrato)
16. [FrameworkConfig — Configuração Global](#16-frameworkconfig--configuração-global)
16B. [Extensões Programáticas](#16b-extensões-programáticas)
17. [Padrões e Recomendações por Camada](#17-padrões-e-recomendações-por-camada)
18. [Exemplos Completos](#18-exemplos-completos)
19. [Orquestração com Databricks Workflows](#19-orquestração-com-databricks-workflows)
20. [Troubleshooting](#20-troubleshooting)
21. [FAQ](#21-faq)
22. [Checklist Pré-Produção](#22-checklist-pré-produção)
23. [Matriz de Compatibilidade](#23-matriz-de-compatibilidade)
24. [Licença e Contribuição](#24-licença-e-contribuição)

---

## 1. Visão Geral e Conceitos

### 1.1 O que é

O **ContractForge** é uma biblioteca Python que encapsula padrões recorrentes de ingestão em Delta Lake, fornecendo uma interface declarativa. Em vez de escrever scripts ad-hoc com `MERGE INTO`, `INSERT`, `OVERWRITE`, Autoloader e controle operacional manual, você descreve **o que** quer fazer via um **contrato declarativo** (`IngestionPlan`), e o framework executa **como** fazer de forma padronizada, com observabilidade completa.

O posicionamento é **contract-first**: o contrato é o artefato versionável que concentra ingestão, schema, qualidade, metadata de catálogo, operações e acesso. A separação em `*.ingestion.yaml`, `*.annotations.yaml`, `*.operations.yaml` e `*.access.yaml` permite que engenharia, governança, SRE e segurança evoluam suas partes sem acoplar todos os ciclos de revisão.

O framework não compete com DLT/Lakeflow como orquestrador gerenciado. Ele ocupa o espaço de biblioteca declarativa com controle fino, portabilidade entre jobs/notebooks/DAB e evidências operacionais persistidas em Delta.

### 1.2 O que ele NÃO faz

- **Não orquestra** — agendamento e DAGs ficam com Databricks Workflows, Airflow, DAB, etc.
- **Não substitui DLT** (Delta Live Tables) — é uma alternativa batch declarativa.
- **Não faz streaming contínuo** — a versão atual suporta Autoloader em `available_now`, que é execução finita com checkpoint; processamento contínuo fica fora do escopo.
- **Não substitui IAM/Unity Catalog** — access declarativo aplica ou valida políticas, mas a autoridade continua no catálogo e nos grupos corporativos.
- **Não é um catálogo de qualidade empresarial** — as regras são para gates de pipeline.

### 1.2B Leitura recomendada

- `docs/quickstart.md`: menor fluxo funcional para validar instalação, ingestão e ctrl tables.
- `docs/templates.md`: templates de contratos para cenários comuns.
- `examples/playground/`: contratos completos e validáveis por CLI para cenários comuns.
- `docs/compatibilidade_conectores.md`: matriz de conectores, dependências externas e suporte por runtime.
- `docs/operacao.md`: retenção das ctrl tables, limpeza operacional e VACUUM.
- `docs/performance.md`: recomendações por modo, JDBC, REST, cache e Delta layout.
- `docs/seguranca.md`: tratamento de secrets, explain, OpenLineage, ctrl tables e quarentena.
- `docs/antipadroes.md`: configurações perigosas e alternativas recomendadas.
- `docs/template_projeto.md` e `examples/project_template/`: estrutura inicial para um repositório de dados com DAB.

### 1.3 Arquitetura Medallion e layers custom

O framework vem com convenções para o modelo de camadas Medallion, mas `layer` é uma classificação lógica livre. Use `bronze`, `silver` e `gold` quando fizer sentido; use `stage`, `raw`, `trusted`, `curated`, `sandbox` ou outro nome quando sua organização pedir uma taxonomia diferente.

| Camada | Valor `layer` | Modos típicos | Propósito |
|--------|---------------|---------------|-----------|
| **Bronze** | `"bronze"` | `scd0_append`, `scd0_overwrite`, `scd1_hash_diff` | Captura bruta, preservação, rastreabilidade |
| **Silver** | `"silver"` | `scd1_upsert`, `scd1_hash_diff`, `scd2_historical`, `snapshot_soft_delete` | Padronização, qualidade, consolidação, histórico |
| **Gold** | `"gold"` | `scd0_overwrite`, `scd1_upsert` | Consumo, agregações, modelos semânticos |

> **Restrição:** apenas o valor literal `layer: bronze` rejeita `scd1_upsert`, `scd2_historical` e `snapshot_soft_delete`, porque bronze deve ser orientada a captura. Layers custom não herdam essa restrição automaticamente.

### 1.4 Fluxo de Execução

Cada chamada `ingest()` ou `ingest_plan()` segue este pipeline determinístico:

```
1. Resolve a fonte (tabela ou DataFrame)
2. Lê watermark anterior do ctrl_ingestion_state
3. Prepara o DataFrame:
   → select_columns → column_mapping → shape → filter_expression → custom_keys
   → apply_watermark → deduplicate_by_order → fix_encoding
   → remove colunas técnicas herdadas de execuções ContractForge anteriores
   → adiciona colunas técnicas (ingestion_date, source_system, __run_id)
4. Valida schema policy + regras de modo
5. Avalia quality gates (single-pass aggregation)
6. Se dry_run: retorna sem escrever
7. Executa o motor de escrita (append/overwrite/merge/hash-diff/scd2/snapshot)
8. Atualiza ctrl_ingestion_state (watermark, status)
9. Registra execução em ctrl_ingestion_runs
10. Emite evento OpenLineage (se habilitado)
```

Quando `source` é `SourceSpec` ou `ConnectorSpec(connector="autoloader")`, `ingest_plan()` despacha para `ingest_stream_plan()`: o framework abre um `readStream` Autoloader, executa `trigger(availableNow=True)` e processa cada micro-batch chamando `ingest_plan()` internamente com `source=batch_df`.

O fluxo usa `try/except/finally` — mesmo em falha, as tabelas de controle recebem o registro com `status=FAILED` e stack trace completo em `ctrl_ingestion_errors`.

### 1.5 Nomenclatura de Destino

A tabela alvo é sempre montada como:

```
{catalog}.{target_schema ou layer}.{target_table}
```

Exemplo: `ingest(catalog="main", layer="silver", target_table="c_cliente")` → `main.silver.c_cliente`

Quando a organização física não segue schemas por camada, informe `target_schema`:

```python
ingest(
    source="raw.crm_cliente",
    catalog="main",
    layer="silver",              # camada lógica: validações, presets e observabilidade
    target_schema="crm_curated", # schema físico do Unity Catalog
    target_table="c_cliente",
)
# → main.crm_curated.c_cliente
```

Se o fluxo tiver uma etapa lógica `stage`, declare isso sem forçar o schema físico:

```yaml
source: raw.orders
layer: stage                 # classificação lógica para presets/observabilidade
target_schema: staging_area  # schema físico no catálogo
target_table: stg_orders
mode: scd0_overwrite
```

Em YAML, o mesmo contrato pode usar campos planos ou o bloco `target`:

```yaml
source: raw.crm_cliente
layer: silver
target:
  catalog: main
  schema: crm_curated
  table: c_cliente
```

---

## 2. Instalação

### 2.1 Via PyPI

```bash
pip install contractforge
```

O pacote mantém apenas `PyYAML` como dependência obrigatória. Em Databricks/serverless, `pyspark` e Delta já vêm do runtime e não devem ser resolvidos pelo wheel. Para execução local fora do Databricks, instale o extra Spark:

```bash
pip install "contractforge[spark]"
```

### 2.2 Via Wheel no Databricks

```bash
# Build local
pip install build
python -m build
# → dist/contractforge-2.6.5-py3-none-any.whl

# Upload para UC Volume
databricks fs cp dist/contractforge-2.6.5-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/libs/

# No notebook Databricks:
%pip install /Volumes/<catalog>/<schema>/libs/contractforge-2.6.5-py3-none-any.whl
dbutils.library.restartPython()
```

### 2.3 Desenvolvimento Local

```bash
git clone https://github.com/marquesantero/contractforge.git
cd contractforge
python -m venv .venv
source .venv/bin/activate  # ou .venv\Scripts\Activate.ps1 no Windows
pip install -e ".[dev]"
pytest tests/test_plan.py -v  # testes puros (rápidos, sem Spark)
pytest -v                      # suite completa (requer Java 11+)
```

### 2.4 Pré-requisitos

| Item | Requisito |
|------|-----------|
| Python | >= 3.10 |
| PySpark | >= 3.4, < 4 quando fora do Databricks; fornecido pelo Databricks Runtime em cluster/serverless |
| delta-spark | >= 3.0, < 4 quando fora do Databricks; fornecido pelo Databricks Runtime em cluster/serverless |
| Databricks Runtime | DBR 13.3 LTS+ (recomendado 14+) |
| Java (fora Databricks) | 11+ |
| Permissões UC | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` no catálogo e schema `ops` |

---

## 3. Quick Start

### 3.1 Via Python (`ingest`)

```python
from contractforge import ingest

# DataFrame de exemplo
df = spark.createDataFrame(
    [(1, "Alice", "2024-01-01"), (2, "Bob", "2024-01-02")],
    "id long, nome string, updated_at string"
)

result = ingest(
    source=df,
    target_table="b_clientes",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    notebook_name="hello_ingest",
    explain_mode=True,
)

print(f"Status: {result['status']}")         # SUCCESS
print(f"Linhas escritas: {result['rows_written']}")  # 2
print(f"Run ID: {result['run_id']}")          # UUID
```

### 3.2 Via YAML (contrato declarativo)

Arquivo `contracts/bronze/b_clientes.yaml`:

```yaml
# Identificação
source: raw_clientes
target_table: b_clientes
catalog: main
layer: bronze
mode: scd0_append
source_system: crm
notebook_name: bronze_clientes

# Incremental
watermark_columns: updated_at

# Schema
schema_policy: permissive

# Metadados
description: "Captura bruta de clientes do CRM"
owner: data-platform
domain: comercial
tags: [bronze, cliente, crm]
```

Notebook genérico que carrega o YAML:

```python
import yaml
from contractforge import ingest_plan
from contractforge.plan import build_plan_from_kwargs

with open("contracts/bronze/b_clientes.yaml") as f:
    cfg = yaml.safe_load(f)

plan = build_plan_from_kwargs(**cfg)
result = ingest_plan(plan)

print(f"Status: {result['status']}")
print(f"Linhas escritas: {result['rows_written']}")
print(f"Run ID: {result['run_id']}")
```

---

## 4. API Pública

```python
from contractforge import (
    ingest,              # Função procedural (kwargs)
    ingest_plan,         # Função recebendo IngestionPlan
    ingest_stream_plan,  # Execução de SourceSpec/ConnectorSpec Autoloader available_now
    IngestionPlan,       # Dataclass do contrato
    SourceSpec,          # Source declarativo legado para Autoloader
    ConnectorSpec,       # Source declarativo genérico via conectores
    QualityRules,        # Dataclass das regras de qualidade
    QualityExpression,   # Regra SQL declarativa com severidade
    FrameworkConfig,     # Configuração global (monkey-patch)
    IngestionHooks,      # Hooks opcionais de execução
    register_write_mode, # Registro de motores de escrita customizados
    register_quality_rule, # Registro de regras de qualidade customizadas
    register_source_resolver, # Registro de resolvers de source customizados
    validate_plan_shape, # Validação pura de contrato sem Spark
)
```

### 4.1 `ingest(**kwargs)` vs `ingest_plan(plan)`

| Cenário | Use `ingest()` | Use `ingest_plan()` |
|---------|----------------|---------------------|
| Notebook exploratório / ad-hoc | ✅ | |
| Job padronizado com YAML | | ✅ |
| Configuração gerada programaticamente | | ✅ |
| Testes unitários | | ✅ |
| Migração de notebooks existentes | ✅ | |

**`ingest(**kwargs)`** — recebe parâmetros como keyword arguments e constrói internamente um `IngestionPlan`. Aceita strings com `|` como separador de listas (conveniente para widgets Databricks). Rejeita parâmetros desconhecidos (protege contra typos).

**`ingest_plan(plan)`** — recebe uma instância de `IngestionPlan` já construída. Ideal quando o plano vem de YAML, JSON ou é construído programaticamente.

### 4.2 Retorno da Execução

Ambas as funções retornam um `dict` com a seguinte estrutura:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `status` | `str` | `"SUCCESS"`, `"FAILED"`, `"DRY_RUN"` ou `"SKIPPED"` |
| `run_id` | `str` | UUID v4 identificador único da execução |
| `target_table` | `str` | Nome completo da tabela alvo (`cat.<target_schema ou layer>.tbl`) |
| `target_schema` | `str` | Schema físico resolvido do target |
| `source_table` | `str` | Nome da fonte ou `"dataframe"` |
| `mode` | `str` | Modo de escrita usado |
| `rows_read` | `int` | Linhas lidas após preparação |
| `rows_written` | `int` | Linhas consideradas na escrita |
| `rows_inserted` | `int` | Linhas inseridas |
| `rows_updated` | `int` | Linhas atualizadas |
| `rows_deleted` | `int` | Linhas removidas/marcadas |
| `rows_quarantined` | `int` | Linhas enviadas à quarentena |
| `watermark_previous` | `str` or `None` | Watermark antes da execução |
| `watermark_current` | `str` or `None` | Watermark após execução |
| `quality_status` | `str` | `"PASSED"`, `"FAILED"`, `"WARNED"`, `"NOT_CONFIGURED"`, `"SKIPPED"` |
| `schema_changes` | `dict` | `{status, added_columns, removed_columns, type_changes}` |
| `operation_metrics` | `dict` | Métricas do histórico Delta |
| `metrics_source` | `str` | `"logical"` (calculado) ou `"mixed"` (Delta + library) |
| `stage_durations` | `dict` | Duração por etapa (`"read"`, `"prepare"`, `"schema"`, `"quality"`, `"write"`, etc.) |
| `write_committed` | `bool` | Indica se houve commit Delta |
| `delta_version_before` | `int` or `None` | Versão Delta antes da escrita |
| `delta_version_after` | `int` or `None` | Versão Delta após a escrita |
| `write_delta_version` | `int` or `None` | Versão Delta do commit de escrita |
| `explain_captured` | `bool` | Se o explain foi capturado |
| `openlineage_event_emitted` | `bool` | Se o evento OpenLineage foi persistido |
| `openlineage_event` | `dict` or `None` | Evento OpenLineage completo |
| `error_message` | `str` or `None` | Mensagem curta de erro |
| `idempotency_key` | `str` or `None` | Chave de idempotência usada |
| `idempotency_policy` | `str` | Política de idempotência |
| `skip_reason` | `str` or `None` | Motivo do skip (idempotência) |
| `skipped_by_run_id` | `str` or `None` | Run que causou o skip |
| `contract_metadata` | `dict` | `{description, owner, domain, tags, sla, runtime_parameters}` |
| `framework_version` | `str` | Versão da biblioteca |
| `ctrl_schema_version` | `int` | Versão do schema das ctrl tables |
| `runtime_type` | `str` | `"classic"` ou `"serverless"` |
| `spark_version` | `str` or `None` | Versão do Spark |
| `python_version` | `str` | Versão do Python |

Para `SourceSpec`/`ConnectorSpec` Autoloader, o retorno externo usa `stream_run_id` em vez de `run_id`, inclui `batches_processed`, `total_rows_read`, `total_rows_written`, `total_rows_quarantined` e `batch_results`. Cada item em `batch_results` é o retorno normal de `ingest_plan()` de um micro-batch.

**Consumo típico:**

```python
result = ingest(...)
if result["status"] != "SUCCESS":
    raise RuntimeError(f"Ingestão falhou: {result.get('error_message', 'desconhecido')}")
print(f"Escritas: {result['rows_written']}, Quarentena: {result['rows_quarantined']}")
```

---

## 5. Referência Completa de Parâmetros do IngestionPlan

O `IngestionPlan` é uma dataclass **frozen** (imutável após construção). Todos os parâmetros são opcionais exceto `source` e `target_table`. A função `ingest()` aceita os mesmos parâmetros como kwargs e os normaliza automaticamente.

### 5.1 Identificação da Execução

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `source` | `str \| DataFrame \| SourceSpec \| ConnectorSpec` | (obrigatório) | Origem: nome de tabela Unity Catalog, DataFrame Spark, Autoloader `available_now` ou conector declarativo |
| `target_table` | `str` | (obrigatório) | Nome da tabela alvo **sem** catálogo/schema. Ex.: `"c_cliente"` |
| `catalog` | `str` | `"main"` | Catálogo Unity Catalog onde alvo e ctrl tables residem |
| `layer` | `str` | `"bronze"` | Classificação lógica para presets, restrições e observabilidade. Bronze/Silver/Gold são convenções, não enum fechado |
| `target_schema` | `str \| None` | `None` | Schema físico do target. Quando omitido, usa `layer` |
| `mode` | `WriteMode` | `"scd0_append"` | Estratégia de escrita (ver §6) |
| `source_system` | `str` | `"default"` | Identificador da origem, gravado como metadado técnico |
| `ctrl_schema` | `str` | `"ops"` | Schema onde as tabelas de controle são criadas |
| `notebook_name` | `str` | `"unknown"` | Nome lógico do notebook/job para auditoria e OpenLineage |

### 5.2 Seleção, Filtro e Preparação

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `select_columns` | `str \| List[str]` | `[]` | Colunas a selecionar da origem. Como string, usa `\|` como separador |
| `column_mapping` | `Dict[str, str]` | `{}` | Renomeia colunas da origem para o alvo após seleção. Ex.: `{src_id: id}` |
| `filter_expression` | `str \| None` | `None` | Expressão SQL aplicada com `.where()` (ex.: `"status != 'CANCELADO'"`) |
| `custom_keys` | `Dict[str, str \| List[str]]` | `{}` | Cria colunas derivadas por concatenação. Ex.: `{"id_item": ["empresa", "filial", "item"]}` → `"empresa|filial|item"` |

### 5.3 Chaves e Deduplicação

| Parâmetro | Tipo | Default | Usado por | Descrição |
|-----------|------|---------|-----------|-----------|
| `merge_keys` | `str \| List[str]` | `[]` | `scd1_upsert`, `scd2_historical`, `snapshot_soft_delete` | Chave(s) natural(is) do MERGE |
| `hash_keys` | `str \| List[str]` | `[]` | `scd1_hash_diff` | Chave(s) para comparar versão mais recente no target |
| `hash_exclude_columns` | `str \| List[str]` | `[]` | `scd1_hash_diff` | Colunas ignoradas no cálculo de hash (ex.: timestamps voláteis) |
| `dedup_order_expr` | `str \| None` | `None` | Todos com chave | Expressão SQL de `ORDER BY` para desempate. Ex.: `"updated_at DESC NULLS LAST"` |

### 5.4 Watermark

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `watermark_columns` | `str \| List[str]` | `[]` | Coluna(s) para carga incremental. Suporta watermark composto (múltiplas colunas) |

### 5.5 Particionamento, Cluster e Otimização

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `partition_column` | `str \| None` | `None` | Coluna de partição física Delta |
| `partition_value` | `str \| None` | `None` | Valor da partição para overwrite parcial |
| `merge_strategy` | `"delta" \| "delta_by_partition" \| "replace_partitions"` | `"delta"` | Estratégia do MERGE em `scd1_upsert` (ver §10) |
| `merge_partition_column` | `str \| None` | `None` | Coluna usada para limitar escopo do merge por partições afetadas |
| `replace_partitions_source_complete` | `bool` | `False` | Confirma que source contém estado completo das partições (obrigatório em `replace_partitions`) |
| `cluster_columns` | `str \| List[str]` | `[]` | Colunas para Delta Liquid Clustering (mutuamente exclusivo com `partition_column`) |
| `zorder_columns` | `str \| List[str]` | `[]` | Colunas para `OPTIMIZE ZORDER BY` |
| `optimize_after_write` | `bool` | `False` | Executa `OPTIMIZE` após escrita com linhas > 0 |
| `delta_properties` | `Dict[str, str]` | `{}` | Propriedades aplicadas na criação da tabela Delta. Ex.: `delta.enableChangeDataFeed=true` |

### 5.6 Schema

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `schema_policy` | `"permissive" \| "additive_only" \| "strict"` | `"permissive"` | Política de evolução de schema (ver §8) |
| `allow_type_widening` | `bool` | `False` | Permite alargamento seguro de tipos (`int→bigint`, `float→double`, etc.) |

### 5.7 Quality Gates

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `quality_rules` | `QualityRules \| dict \| None` | `None` | Regras de qualidade avaliadas antes da escrita (ver §7) |
| `on_quality_fail` | `"fail" \| "warn" \| "quarantine"` | `"fail"` | Ação quando regras de qualidade falham |

### 5.8 SCD2 — Histórico

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `scd2_change_columns` | `str \| List[str]` | `[]` | Colunas cuja mudança gera nova versão histórica. Se vazio: todas exceto chaves e controle |
| `scd2_effective_from_column` | `str \| None` | `None` | Coluna da origem usada como `valid_from`. Se omitida: `current_timestamp()` |

### 5.9 Encoding

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `fix_encoding` | `bool` | `False` | Ativa correção de encoding em colunas string |
| `encoding` | `str` | `"Windows-1252"` | Encoding de origem para correção |
| `encoding_columns` | `str \| List[str]` | `[]` | Colunas a corrigir. Se vazio: todas as colunas string |

### 5.10 Diagnóstico e Observabilidade

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `dry_run` | `bool` | `False` | Valida tudo sem escrever nem criar ctrl tables. Retorna `status="DRY_RUN"` |
| `explain_mode` | `bool` | `False` | Captura `df.explain()` e persiste em `ctrl_ingestion_explain` |
| `explain_format` | `str` | `"formatted"` | Formato do explain: `"simple"`, `"extended"`, `"formatted"`, `"cost"`, `"codegen"` |
| `openlineage_enabled` | `bool` | `False` | Gera e persiste evento OpenLineage em JSON |
| `openlineage_namespace` | `str \| None` | `None` | Namespace OpenLineage. Default: `databricks://<catalog>` |
| `openlineage_producer` | `str` | `"contractforge"` | Identificador do produtor no evento OpenLineage |

### 5.11 Performance e Concorrência

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `use_cache` | `bool` | `True` | Cacheia o DataFrame preparado com `.cache()`. Desabilitado automaticamente em serverless |
| `lock_enabled` | `bool` | `False` | Ativa lock operacional best-effort por `target_table` |
| `retry_attempts` | `int \| None` | `None` | Sobrescreve o número de tentativas para conflitos Delta neste plano |
| `retry_backoff_seconds` | `int \| None` | `None` | Sobrescreve o backoff base entre tentativas neste plano |
| `hooks` | `IngestionHooks \| None` | `None` | Callbacks opcionais `before_read`, `after_prepare`, `before_write`, `after_write` |

### 5.12 Idempotência

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `idempotency_key` | `str \| None` | `None` | Chave lógica do lote. Identifica unicamente uma carga |
| `idempotency_policy` | `"always_run" \| "skip_if_success" \| "fail_if_success" \| "rerun_if_failed"` | `"always_run"` | Comportamento ao reencontrar `idempotency_key` |

### 5.13 Linhagem Operacional

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `parent_run_id` | `str \| None` | `None` | ID da execução pai (DAGs, sub-jobs) |
| `run_group_id` | `str \| None` | `None` | ID lógico do grupo de execução |
| `master_job_id` | `str \| None` | `None` | ID do job mestre no orquestrador |
| `master_run_id` | `str \| None` | `None` | ID da execução mestre. Ex.: `{{job.run_id}}` do Databricks |

### 5.14 Metadados de Contrato

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `description` | `str \| None` | `None` | Descrição do contrato de ingestão |
| `owner` | `str \| None` | `None` | Dono/responsável pelo pipeline |
| `domain` | `str \| None` | `None` | Domínio de negócio (ex.: `"comercial"`, `"financeiro"`) |
| `tags` | `List[str]` | `[]` | Tags do contrato. String com `\|` também aceita |
| `sla` | `str \| None` | `None` | SLA esperado (ex.: `"D+0 08:00"`) |
| `runtime_parameters` | `Dict[str, Any]` | `{}` | Parâmetros de execução arbitrários, propagados nas ctrl tables e retorno |

---

## 5B. Anatomia de um Contrato YAML Completo

Cada tabela vira um arquivo YAML. Abaixo, um contrato anotado com todos os campos disponíveis e comentários explicativos:

```yaml
# ============================================================
# contracts/silver/c_cliente.yaml
# Contrato completo de ingestão — Silver SCD1 com todos os recursos
# ============================================================

# --- Obrigatórios ---
source: b_cliente                        # str: nome de tabela Unity Catalog
target_table: c_cliente                  # str: nome da tabela alvo (sem catalog/schema)

# --- Identificação do ambiente ---
catalog: main                            # default: "main"
layer: silver                            # camada lógica: bronze/silver/gold ou custom, ex.: stage/raw/curated
target_schema: crm_curated               # opcional; default = layer
mode: scd1_upsert                        # modo de escrita (ver §6)
source_system: crm                       # default: "default"
ctrl_schema: ops                         # default: "ops" — schema das ctrl tables
notebook_name: ingest_silver_clientes    # default: "unknown"

# --- Metadados de contrato (propagados para ctrl tables) ---
description: "Clientes consolidados do CRM com deduplicação"
owner: data-platform
domain: comercial
tags: [silver, cliente, crm]             # lista ou "silver|cliente|crm"
sla: "D+0 08:00"
runtime_parameters:
  carga: incremental
  prioridade: alta

# --- Transformações ---
select_columns: []                       # opcional: filtrar colunas. Ex.: "id|nome|email"
column_mapping: {}                       # opcional: origem -> alvo. Ex.: {cod_cli: id_cliente}
filter_expression: null                  # opcional: SQL WHERE. Ex.: "status != 'CANCELADO'"
custom_keys: {}                          # opcional: chaves derivadas. Ex.: {id_item: [empresa, filial, item]}

# --- Chaves e deduplicação ---
merge_keys: id_cliente                   # obrigatório em scd1_upsert/scd2/snapshot
# hash_keys: id_cliente                  # alternativo em scd1_hash_diff
# hash_exclude_columns: updated_at|extraction_ts
dedup_order_expr: "updated_at DESC NULLS LAST"

# --- Watermark (carga incremental) ---
watermark_columns: updated_at            # simples: "coluna". Composto: "c1|c2|c3"

# --- Layout Delta ---
# partition_column: ingestion_date       # partição física (cuidado com cardinalidade)
# partition_value: null                  # usado em overwrite por partição
merge_strategy: delta                    # delta | delta_by_partition | replace_partitions
# merge_partition_column: dt
# replace_partitions_source_complete: false
cluster_columns: []                      # Delta Liquid Clustering. Ex.: "id_cliente|status"
zorder_columns: []                       # ZORDER. Ex.: "id_cliente|updated_at"
optimize_after_write: false              # executa OPTIMIZE após escrita
delta_properties: {}                     # Ex.: {delta.enableChangeDataFeed: "true"}

# --- Schema ---
schema_policy: additive_only             # permissive | additive_only | strict
allow_type_widening: false               # int→bigint, float→double, etc.

# --- Quality gates ---
quality_rules:
  required_columns: [id_cliente, updated_at]
  not_null: [id_cliente]
  unique_key: [id_cliente]
  accepted_values:
    status: [ATIVO, INATIVO, PENDENTE]
  min_rows: 1
  max_null_ratio:
    email: 0.15
    telefone: 0.30
  expressions:
    - name: positive_amount
      expression: "amount > 0"
      severity: quarantine          # warn | quarantine | abort
      message: "Valor deve ser positivo."
    - name: valid_period
      expression: "end_date >= start_date OR end_date IS NULL"
      severity: abort
      message: "Período inválido."
on_quality_fail: fail                   # fail | warn | quarantine

# --- SCD2 (apenas se mode=scd2_historical) ---
# scd2_change_columns: nome|email|status
# scd2_effective_from_column: updated_at

# --- Encoding ---
fix_encoding: false
# encoding: Windows-1252
# encoding_columns: []

# --- Diagnóstico ---
dry_run: false
explain_mode: false
explain_format: formatted               # simple | extended | formatted | cost | codegen

# --- OpenLineage ---
openlineage_enabled: true
openlineage_namespace: databricks://main
openlineage_producer: contractforge

# --- Performance ---
use_cache: true                         # cacheia DataFrame preparado (desabilitado em serverless)
lock_enabled: false                     # lock best-effort por target_table
retry_attempts: null                    # sobrescreve default global se informado
retry_backoff_seconds: null             # sobrescreve default global se informado

# --- Idempotência ---
# idempotency_key: "job-42:batch-2026-05-11"
idempotency_policy: always_run           # always_run | skip_if_success | fail_if_success | rerun_if_failed

# --- Linhagem operacional (preenchidos pelo orquestrador) ---
# parent_run_id: null
# run_group_id: null
# master_job_id: null
# master_run_id: null
```

---

## 5C. Fontes e Conectores Declarativos

Além de tabela e `DataFrame`, `source` aceita fontes declarativas. O formato antigo `SourceSpec` continua disponível para Auto Loader, mas o formato recomendado é `ConnectorSpec`:

```yaml
source:
  type: connector
  connector: <nome_do_conector>
```

Conectores nativos:

| Conector | Uso | Campos principais |
|----------|-----|-------------------|
| `table`, `delta_table`, `view` | Tabelas/views do catálogo Spark/Unity Catalog | `table` |
| `sql` | Query SQL declarativa | `query` |
| `parquet`, `delta`, `json`, `csv`, `orc`, `text` | Arquivos batch | `path`, `options` |
| `http_file`, `http_csv`, `http_json`, `http_text` | Arquivos HTTP(S) materializados pelo driver Python | `path` ou `request.url`, `format`, `options` |
| `object_storage`, `blob`, `s3`, `adls`, `azure_blob`, `gcs` | Arquivos em ADLS/Azure Blob/S3/GCS | `provider` opcional nos aliases, `format`, `path`, `options` |
| `jdbc`, `postgres`, `postgresql`, `sqlserver`, `mysql`, `oracle` | Bancos relacionais via Spark JDBC | `options.url`, `options.dbtable` ou `options.query` |
| `snowflake`, `bigquery` | Conectores Spark externos instalados no runtime | `table`, `query`, `options.table`, `options.dbtable` ou `options.query` |
| `rest_api` | APIs REST JSON em batch | `request`, `auth`, `pagination`, `response`, `limits` |
| `autoloader` | Auto Loader finito `available_now` | `path`, `format`, `read.schema_location`, `read.checkpoint_location` |

O retorno de `ingest()` inclui `source` com metadados do conector. `ctrl_ingestion_runs` registra `source_connector`, `source_provider`, `source_format`, `source_path`, configurações redigidas, capabilities do source e métricas operacionais em `source_metrics_json`.

`source_metrics_json` é preenchido pelo resolver do conector. Em REST, inclui quantidade de requests, páginas lidas, registros extraídos, bytes lidos, tipo de paginação, retry/rate limit e watermark aplicado. Em HTTP file, inclui formato, registros materializados, bytes baixados e retry. Em JDBC e aliases nomeados, inclui estratégia de leitura, se houve pushdown incremental, watermark aplicado, particionamento, `fetchsize`, tipo de autenticação e, para RDS IAM, a região e se o token foi gerado. Em fontes Spark nativas, registra a estratégia (`spark_table`, `spark_sql`, `spark_files` ou `spark_format`) e se a fonte foi declarada como completa.

`contractforge validate` faz validação estática dos conectores nativos sem abrir Spark: campos obrigatórios, tipos de paginação REST, auth REST, particionamento JDBC e formatos de object storage são verificados antes do job.

Descoberta via CLI:

```bash
contractforge connectors list
contractforge connectors show rest_api http_file postgres s3 bigquery autoloader
contractforge connectors doctor rest_api http_file postgres s3 bigquery autoloader
```

`connectors doctor` é diagnóstico estático: não abre conexão, não cria SparkSession e não valida credenciais. Ele informa se o conector depende de recurso do runtime, como Auto Loader, driver JDBC, connector Spark externo ou configuração cloud.

### 5C.1 Auto Loader

Formato recomendado:

```yaml
source:
  type: connector
  connector: autoloader
  path: /Volumes/main/landing/orders
  format: json
  read:
    schema_location: /Volumes/main/ops/autoloader_schemas/orders
    checkpoint_location: /Volumes/main/ops/checkpoints/orders
    include_existing_files: true
    max_files_per_trigger: 1000
    schema_hints: "order_id BIGINT, amount DECIMAL(18,2)"
  options:
    cloudFiles.inferColumnTypes: "true"

target_table: b_orders
catalog: main
layer: bronze
mode: scd0_append
schema_policy: additive_only
notebook_name: bronze_orders_autoloader
```

Formato legado equivalente:

```python
from contractforge import SourceSpec, ingest

result = ingest(
    source=SourceSpec(
        type="autoloader",
        path="/Volumes/main/landing/orders",
        format="json",
        schema_location="/Volumes/main/ops/autoloader_schemas/orders",
        checkpoint_location="/Volumes/main/ops/checkpoints/orders",
        options={"cloudFiles.inferColumnTypes": "true"},
    ),
    target_table="b_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
)
```

Semântica operacional:

- O framework usa `spark.readStream.format("cloudFiles")` e `trigger(availableNow=True)`.
- A execução externa é registrada em `ctrl_ingestion_streams`.
- Cada micro-batch vira uma execução filha em `ctrl_ingestion_runs`, com `parent_run_id = stream_run_id`.
- `idempotency_key` no stream gera chaves de batch no formato `<idempotency_key>:batch:<batch_id>`.
- `snapshot_soft_delete` não deve ser usado com Auto Loader; Auto Loader entrega arquivos incrementais, não snapshot completo.
- Streaming contínuo não é suportado nesta versão; o contrato é deliberadamente finito.

### 5C.2 Arquivos e Object Storage

Leitura batch de JSON em Volume:

```yaml
source:
  type: connector
  connector: json
  name: landing_orders_json
  path: /Volumes/main/landing/orders_json
  options:
    multiline: true
    inferSchema: true

target_table: b_orders_json
catalog: main
layer: bronze
mode: scd0_append
schema_policy: additive_only
```

Leitura em S3/ADLS/GCS usa o mesmo mecanismo Spark, deixando credenciais, external locations, mounts ou profiles sob responsabilidade do runtime:

```yaml
source:
  type: connector
  connector: s3
  format: parquet
  path: s3://company-landing/orders/
  read:
    source_complete: true

target_table: snapshot_orders
catalog: main
layer: silver
mode: snapshot_soft_delete
merge_keys: [order_id]
```

Você pode usar aliases diretos (`s3`, `adls`, `azure_blob`, `gcs`) ou o formato genérico:

```yaml
source:
  type: connector
  connector: object_storage
  provider: gcs
  format: json
  path: gs://company-landing/events/
  options:
    multiline: true
```

`provider` aceita `adls`, `azure_blob`, `s3` e `gcs`. Para paths já governados por Unity Catalog/External Location/Volumes, a lib valida o contrato e delega credenciais ao runtime Spark.

Em Databricks serverless, prefira External Location/Volume:

```yaml
source:
  type: connector
  connector: azure_blob
  path: abfss://landing@exampleacct.dfs.core.windows.net/datasets/csv/orders.csv
  format: csv
  options:
    header: true
    inferSchema: false
  read:
    source_complete: true
    schema: "order_id STRING, customer_id STRING, order_ts_utc TIMESTAMP, amount DOUBLE"
```

Para `azure_blob`, também é possível declarar SAS diretamente no contrato usando secret placeholder em job cluster/classic/local. Nesse caso, a ContractForge resolve o secret, configura `fs.azure.sas.<container>.<account>.blob.core.windows.net` e monta o path `wasbs://...` automaticamente. O secret pode conter o SAS com ou sem `?` inicial. Esse caminho é apropriado para runtimes onde configuração Hadoop/Spark é permitida.

Para `s3`, também é possível declarar credenciais diretamente em `source.auth` em job cluster/classic/local. A ContractForge resolve secrets, configura `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.session.token` quando existir, e escolhe `SimpleAWSCredentialsProvider` ou `TemporaryAWSCredentialsProvider`.

```yaml
source:
  type: connector
  connector: s3
  path: s3a://company-landing/orders/
  format: csv
  auth:
    access_key_id: "{{ secret:aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:aws/aws_secret_access_key }}"
    session_token: "{{ secret:aws/aws_session_token }}"  # opcional para STS
  options:
    header: true
    fs.s3a.endpoint: s3.us-east-1.amazonaws.com
  read:
    source_complete: true
    schema: "order_id STRING, customer_id STRING, amount DOUBLE"
```

Use `source.auth` para S3 apenas em runtimes onde configuração Hadoop/Spark é permitida. Em Databricks serverless/Spark Connect, se o runtime bloquear `spark.conf.set`, a ContractForge falha rápido com orientação para usar Unity Catalog External Location/Volume. O conector `s3` não executa fallback REST implícito.

Em Databricks serverless/Spark Connect, se o runtime bloquear `spark.conf.set`, a ContractForge falha rápido com orientação para usar Unity Catalog External Location/Volume (`abfss://...`, `s3://...` governado por External Location, ou `/Volumes/...`) ou configurar Serverless Network Policy/NCC para permitir o destino. O conector `azure_blob` não executa fallback REST implícito; para arquivo HTTP(S) explícito de volume controlado, use `http_file`. Para `avro`, `xml`, `parquet`, `delta` e `orc`, a leitura depende do reader Spark e de credencial configurada no runtime/Unity Catalog.

Formatos de arquivo aceitos por conectores de arquivo/object storage: `avro`, `csv`, `delta`, `json`, `jsonl`, `ndjson`, `orc`, `parquet`, `text` e `xml`. `jsonl` e `ndjson` são formatos lógicos da ContractForge e usam o reader Spark `json`. A leitura de `xml` depende de suporte do runtime Spark; Excel não é formato Spark nativo e deve usar um conector específico/runtime externo.

Quando o schema é conhecido, declare `source.read.schema` como DDL Spark. `source.schema` também é aceito como alias curto e é normalizado para `source.read.schema`; declarar ambos com valores diferentes falha antes da leitura. Isso evita inferência em leituras grandes ou com muitos arquivos pequenos e aparece em `source_metrics_json.schema_declared=true`.

### 5C.2B HTTP File

Use `http_file` quando a origem é um arquivo publicado por HTTP(S), mas o runtime Spark não consegue ler `https://` diretamente como filesystem. O conector baixa o conteúdo com Python, parseia o formato declarado e cria o DataFrame Spark. É indicado para arquivos públicos ou autenticados de volume controlado; para alto volume recorrente, prefira landing em storage + Auto Loader.

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

target_table: b_covid_brazil_states
catalog: workspace
layer: bronze
mode: scd0_overwrite
source_system: covid19br_github
```

Formatos suportados: `csv`, `json`, `jsonl`, `ndjson` e `text`. Para JSON, `source.response.records_path` usa o mesmo JSON path simples do `rest_api`: raiz `$`, campos com `$.data.items` e índices inteiros como `$[1]` ou `$.data[0].items`. Não é JSONPath completo; wildcards, filtros e expressões não são suportados.

```yaml
source:
  type: connector
  connector: http_file
  path: https://example.com/export.json
  format: json
  response:
    records_path: $.data[0].items
```

Aliases:

```yaml
source:
  type: connector
  connector: http_csv
  path: https://example.com/data.csv
  options:
    header: true
```

### 5C.3 JDBC e Bancos Nomeados

```yaml
source:
  type: connector
  connector: postgres
  name: erp_orders
  options:
    url: "{{ secret:erp/postgres_url }}"
    dbtable: public.orders
  auth:
    type: basic
    username: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
  read:
    partition_column: id
    lower_bound: 1
    upper_bound: 5000000
    num_partitions: 16
    fetchsize: 10000
    source_complete: true

target_table: b_erp_orders
catalog: main
layer: bronze
mode: scd0_append
```

Aliases `postgres`, `postgresql`, `sqlserver`, `mysql` e `oracle` usam o mesmo executor JDBC, mas deixam o contrato mais explícito e a observabilidade registra o conector real declarado. Os drivers JDBC continuam responsabilidade do runtime.

Para Amazon RDS/Aurora com IAM database authentication, o conector pode gerar o token IAM no driver Python usando `auth.type: rds_iam`. A lib não depende de `boto3` nem AWS CLI para isso; ela assina o token SigV4 a partir das credenciais declaradas, das variáveis `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e `AWS_SESSION_TOKEN`, ou da AWS credential provider chain quando `credential_provider: default_chain` for configurado com `botocore` instalado.

O guia operacional completo está em [RDS/Aurora JDBC com IAM Auth](rds_iam_jdbc.md). Ele cobre driver JDBC, cluster `SINGLE_USER` versus artifact allowlist, `GRANT rds_iam`, policy `rds-db:connect`, secrets e troubleshooting.

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: jdbc:postgresql://database-1.cluster-cgxy0608al48.us-east-1.rds.amazonaws.com:5432/postgres
    dbtable: public.orders
    driver: org.postgresql.Driver
  auth:
    type: rds_iam
    username: postgres
    region: us-east-1
    access_key_id: "{{ secret:contractforge-aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:contractforge-aws/aws_secret_access_key }}"
    session_token: "{{ secret:contractforge-aws/aws_session_token }}"
    sslmode: require
```

Para usar instance profile, web identity ou outro provider suportado por `botocore`, substitua as chaves explícitas por:

```yaml
  auth:
    type: rds_iam
    username: postgres
    region: us-east-1
    credential_provider: default_chain
```

Conectividade continua sendo responsabilidade do runtime. Para RDS/Aurora, use uma das opções suportadas pela plataforma: mesma VPC, VPC peering, Transit Gateway, PrivateLink/NLB, endpoint público tradicional com security group restrito, ou Aurora Express Internet Access Gateway quando esse modo estiver habilitado e acessível a partir do compute.

Validação real já realizada em Databricks classic single-node com Aurora PostgreSQL 17.7 e ContractForge 2.6.5+: `auth.type=rds_iam`, Spark JDBC, particionamento JDBC, quality rules e `scd1_hash_diff` terminaram com `SUCCESS`.

Regras:

- `source.options.url` é obrigatório.
- Informe `source.options.dbtable` ou `source.options.query`.
- Particionamento JDBC exige os quatro campos juntos: `partition_column`, `lower_bound`, `upper_bound`, `num_partitions`.
- Use `source.read.source_complete=true` somente quando a query/tabela representar o estado completo necessário ao modo de escrita.
- Em `ingest()` programático, informe `catalog` explicitamente. `target_schema` qualificado não muda `plan.catalog`.
- `PAM authentication failed` em RDS/Aurora geralmente indica problema de IAM/database auth, não problema de rede.

### 5C.3B Snowflake e BigQuery

`snowflake` e `bigquery` usam `spark.read.format("snowflake")` e `spark.read.format("bigquery")`. A lib valida contrato, resolve secrets, redige opções sensíveis e registra métricas, mas o conector Spark correspondente precisa estar disponível no runtime.

```yaml
source:
  type: connector
  connector: snowflake
  name: sf_orders
  options:
    sfURL: "{{ secret:snowflake/url }}"
    sfUser: "{{ secret:snowflake/user }}"
    sfPassword: "{{ secret:snowflake/password }}"
    sfDatabase: RAW
    sfSchema: PUBLIC
    sfWarehouse: INGEST_WH
    dbtable: ORDERS

target_table: b_snowflake_orders
catalog: main
layer: bronze
mode: scd0_append
```

```yaml
source:
  type: connector
  connector: bigquery
  table: my-project.raw.orders
  options:
    parentProject: my-project

target_table: b_bigquery_orders
catalog: main
layer: bronze
mode: scd0_append
```

### 5C.4 REST API

REST API é batch e materializa a resposta JSON em DataFrame Spark. É adequado para APIs administrativas, catálogos pequenos/médios e endpoints paginados; para alto volume contínuo, prefira landing em arquivos + Auto Loader.

```yaml
source:
  type: connector
  connector: rest_api
  name: orders_api
  request:
    url: https://api.example.com/orders
    method: GET
    params:
      status: open
    headers:
      Accept: application/json
  auth:
    type: bearer_token
    token: "{{ secret:integrations/orders_api_token }}"
  pagination:
    type: cursor
    cursor_param: cursor
    next_cursor_path: $.next
  response:
    records_path: $.data
  incremental:
    watermark_param: updated_after
    initial_value: "1970-01-01T00:00:00Z"
  limits:
    max_pages: 50
    timeout_seconds: 60
    retry_attempts: 3
    retry_backoff_seconds: 2
    rate_limit_per_minute: 120

target_table: b_orders_api
catalog: main
layer: bronze
mode: scd0_append
```

Por padrão, `rest_api` usa `response.mode: records`: a lib aplica `response.records_path`, materializa uma lista de registros e deixa o Spark inferir o schema. `records_path` suporta apenas navegação simples em JSON: `$`, `$.data.items`, `$[0]` e `$.data[0].items`. Em Spark clássico, essa materialização usa JSON lines + `spark.read.json`, por RDD quando disponível ou por staging configurado, o que é mais robusto para payloads REST reais com structs, arrays e campos opcionais heterogêneos. O caminho usado fica registrado em `source_metrics.dataframe_materialization`.

Para APIs com JSON muito heterogêneo, objetos dinâmicos ou campos que podem gerar conflito na inferência do Spark, declare `source.read.schema` ou o alias curto `source.schema`. O schema é repassado ao Spark JSON reader e transforma a API em um contrato explícito de leitura. Isso evita correções específicas por fonte e mantém o tratamento de dados no contrato.

Quando o runtime não expõe `sparkContext` e bloqueia inferência direta por `createDataFrame`, declare um staging de JSON local acessível ao driver Python e ao Spark reader:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://api.example.com/items
  response:
    records_path: $.data
  read:
    staging_path: /Volumes/main/ops/tmp/contractforge_rest_api
    schema: "id STRING, payload STRUCT<status:STRING, amount:DOUBLE>"
    json_options:
      rescuedDataColumn: _rescued_data
      readerCaseSensitive: true
```

Também é possível definir `CONTRACTFORGE_SOURCE_JSON_STAGING_DIR` no ambiente. O staging deve ser um caminho de filesystem que o Python consegue escrever e o Spark consegue ler, como `/Volumes/...`, `/Workspace/...` quando permitido pelo runtime, ou `file:/...`. URIs remotas como `abfss://...` não são aceitas nesse campo porque a escrita é feita pelo driver Python.

Use `source.read.schema` para schemas explícitos e `source.read.json_options` para repassar opções ao Spark JSON reader usado nessa materialização. Isso é útil para recursos do runtime como coluna de resgate, tratamento de case-sensitivity, permissividade de parser e formatos de data/hora.

Use `response.mode: raw` quando a resposta precisa ser tratada como documento completo por página, quando você quer controlar o schema explicitamente com `shape.parse_json`, ou quando o volume/payload é grande demais para materialização direta em memória. Nesse modo o conector não transforma os registros: ele grava uma linha por página com o payload bruto em uma coluna string. O tratamento fica no `shape`.

```yaml
source:
  type: connector
  connector: rest_api
  name: nasa_eonet_events
  request:
    url: https://eonet.gsfc.nasa.gov/api/v3/events
    method: GET
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

Limites de segurança:

- `limits.max_page_bytes`: falha se uma página/resposta individual exceder o tamanho declarado.
- `limits.max_total_bytes`: falha se a soma das respostas da execução exceder o tamanho declarado.
- Para payloads grandes, recorrentes ou que exigem replay completo, prefira landing em storage e Auto Loader. O conector REST roda no driver Python e é intencionalmente voltado a APIs pequenas/médias.

Autenticação suportada:

- `none`
- `bearer_token` com `token`
- `api_key` com `header` e `value`/`key`
- `basic` com `username` e `password`
- `oauth_client_credentials` com `token_url`, `client_id`, `client_secret` e `scope` opcional

Paginação suportada:

- `none`: uma requisição.
- `page`: incrementa `page_param`.
- `offset`: incrementa `offset_param` com `page_size`.
- `cursor`: lê cursor em `next_cursor_path` e envia em `cursor_param`.
- `link_header`: segue o header HTTP `Link` com `rel="next"`.

Extração de registros usa JSON path simples no formato `$.campo.subcampo`, por exemplo `$.data.items`.

Pushdown incremental:

- `source.incremental.watermark_param`: injeta o watermark anterior como query param.
- `source.incremental.watermark_header`: injeta o watermark anterior como header HTTP.
- `source.incremental.watermark_body_field`: injeta o watermark anterior em `request.json` para chamadas `POST`.
- `source.incremental.initial_value`: valor usado apenas quando ainda não existe watermark salvo.

O pushdown incremental não substitui `watermark_columns`; ele só reduz o volume lido da origem. O watermark oficial continua sendo calculado após prepare/quality com base em `watermark_columns`.

### 5C.4B JDBC Incremental

JDBC também aceita pushdown incremental:

```yaml
watermark_columns: updated_at

source:
  type: connector
  connector: jdbc
  options:
    url: "{{ secret:erp/jdbc_url }}"
    dbtable: public.orders
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
  incremental:
    watermark_column: updated_at
    initial_value: "1970-01-01 00:00:00"
```

Quando houver watermark anterior, o conector transforma `dbtable` em subquery com `WHERE updated_at > '<watermark>'`. Para predicados customizados:

```yaml
source:
  type: connector
  connector: jdbc
  options:
    url: "{{ secret:erp/jdbc_url }}"
    query: "SELECT * FROM public.orders WHERE status <> 'CANCELLED'"
  incremental:
    predicate: "updated_at >= TIMESTAMP '{watermark_previous}'"
```

### 5C.5 Secrets e Observabilidade

Use placeholders `{{ secret:scope/key }}` em `options`, `request`, `auth`, `pagination`, `response` ou `limits`. A resolução tenta primeiro a variável de ambiente `CONTRACTFORGE_SECRET_SCOPE_KEY`; se não existir, usa Databricks Secrets via `dbutils.secrets.get(scope, key)`.

Os valores sensíveis são redigidos em logs e ctrl tables. A auditoria de execução persiste configurações redigidas em:

- `source_options_json`
- `source_read_json`
- `source_request_json`
- `source_auth_json`
- `source_pagination_json`
- `source_response_json`
- `source_incremental_json`
- `source_limits_json`
- `source_capabilities_json`
- `source_metrics_json`

### 5C.6 Extensão

Novos conectores podem ser acoplados sem alterar o dispatcher principal:

```python
from contractforge import ConnectorCapabilities, SourceResolution, register_source_resolver

class MyConnector:
    def capabilities(self, spec):
        return ConnectorCapabilities(batch=True, source_complete=False)

    def resolve_batch(self, spec, plan):
        df = ...
        return SourceResolution(
            df=df,
            label=f"my_connector:{spec.name}",
            connector=spec.connector,
            metadata={"source_connector": spec.connector},
            capabilities=self.capabilities(spec),
        )

register_source_resolver("my_connector", MyConnector())
```

---

## 5D. Presets Declarativos

Presets são defaults opinativos para padrões comuns de ingestão. Eles existem para reduzir repetição nos YAMLs, mantendo o contrato auditável: o campo explícito no contrato sempre sobrescreve o valor definido pelo preset.

### 5D.1 Regras de Aplicação

- `preset` aceita string ou lista de strings.
- Presets são aplicados na ordem declarada.
- Apenas um preset principal de ingestão pode ser usado por contrato.
- Apenas um preset de runtime pode ser usado por contrato.
- Modificadores de quality, Delta e governança podem ser combinados com um preset de ingestão.
- Dicionários fazem merge profundo; listas são substituídas pelo contrato explícito.
- O resultado de `ingest()` inclui `applied_presets` para auditoria.

### 5D.2 Exemplo YAML

```yaml
preset:
  - runtime_databricks_serverless
  - delta_cdf_enabled
  - silver_scd1_upsert
  - quality_quarantine

source: raw.orders
target_table: s_orders
catalog: main
merge_keys: order_id
quality_rules:
  not_null: [order_id]
```

Contrato expandido efetivo:

```yaml
layer: silver
mode: scd1_upsert
merge_strategy: delta
schema_policy: additive_only
on_quality_fail: quarantine
delta_properties:
  delta.enableChangeDataFeed: "true"
```

### 5D.3 Presets de Ingestão

| Preset | Camada | Modo/estratégia | Uso principal |
|--------|--------|-----------------|---------------|
| `bronze_autoloader_append` | Bronze | `scd0_append` + Autoloader declarativo | Arquivos em landing/raw via Auto Loader `available_now` |
| `bronze_file_append` | Bronze | `scd0_append` | Batch de arquivos/DataFrame já resolvido |
| `bronze_table_append` | Bronze | `scd0_append` | Replicação simples table-to-table |
| `bronze_full_overwrite` | Bronze | `scd0_overwrite` | Snapshot completo pequeno/médio |
| `bronze_partition_overwrite` | Bronze | `scd0_overwrite` por partição | Reprocessamento de partição |
| `silver_scd1_upsert` | Silver | `scd1_upsert` + `delta` | Estado atual por chave |
| `silver_scd1_partition_upsert` | Silver | `scd1_upsert` + `delta_by_partition` | Upsert grande com poda por partição |
| `silver_replace_partitions` | Silver | `scd1_upsert` + `replace_partitions` | Source completo por partição |
| `silver_hash_diff_append` | Silver | `scd1_hash_diff` | Registrar apenas mudanças reais |
| `silver_snapshot_soft_delete` | Silver | `snapshot_soft_delete` | Sincronizar snapshot completo com inativação |
| `silver_scd2_historical` | Silver | `scd2_historical` | Histórico de alterações |
| `silver_incremental_watermark_upsert` | Silver | `scd1_upsert` + watermark | Incremental por timestamp/versão |
| `silver_quarantine_ingestion` | Silver | `scd1_upsert` + quarantine | Ingestão tolerante a erro linha-a-linha |
| `gold_full_refresh` | Gold | `scd0_overwrite` | Tabela agregada recalculada inteira |
| `gold_partition_refresh` | Gold | `scd0_overwrite` por partição | Recalcular partição diária/mensal |
| `gold_replace_partitions` | Gold | `scd1_upsert` + `replace_partitions` | Fatos/agregados por partição |
| `gold_snapshot_serving` | Gold | `snapshot_soft_delete` | Serving com estado ativo/inativo |
| `gold_scd1_serving` | Gold | `scd1_upsert` | Serving corrente sem histórico |

### 5D.4 Modificadores

| Preset | Categoria | Efeito |
|--------|-----------|--------|
| `quality_strict` | Quality | `on_quality_fail=fail` |
| `quality_quarantine` | Quality | `on_quality_fail=quarantine` |
| `delta_cdf_enabled` | Delta | `delta.enableChangeDataFeed=true` |
| `delta_optimized_writes` | Delta | `delta.autoOptimize.optimizeWrite=true` e `delta.autoOptimize.autoCompact=true` |
| `runtime_databricks_serverless` | Runtime | Defaults conservadores para Serverless/Spark Connect |
| `runtime_spark_delta_local` | Runtime | Defaults conservadores para testes locais |
| `governance_uc_basic` | Governança | `annotations.policy=warn` e `access.mode=validate_only` |

### 5D.5 CLI e Extensão

```bash
contractforge presets list
contractforge presets show silver_scd1_upsert
contractforge templates list
contractforge templates write silver_jdbc_scd1_upsert --output contracts/silver/s_orders
contractforge connectors list
contractforge connectors show rest_api postgres s3 bigquery
contractforge connectors doctor rest_api postgres s3 bigquery
contractforge validate contracts/silver/orders.yaml --expand-presets
```

```python
from contractforge import register_preset

register_preset("company_silver_default", {
    "layer": "silver",
    "mode": "scd1_upsert",
    "schema_policy": "additive_only",
    "on_quality_fail": "quarantine",
})
```

---

## 5E. Shape Declarativo para JSON, Structs e Arrays

`shape` transforma a estrutura física do DataFrame antes de filtros, watermark, dedup, quality e escrita. Ele é separado de `annotations`: `shape` altera dados/colunas; `annotations` descreve catálogo.

### 5E.1 Quando Usar

- Bronze: preservar o bruto por padrão. Use `to_json`, `size` ou `first` quando quiser enriquecer sem mudar cardinalidade.
- Silver: local recomendado para `flatten`, `explode` e normalização de JSON/arrays.
- Gold: usar apenas para serving final quando a Silver ainda não entregar a forma esperada.

### 5E.2 Parse de JSON em Coluna String

Quando o JSON já chega como `struct`/`array`, `columns`, `arrays` e `flatten` atuam diretamente no schema. Quando o payload chega como texto (`string`), declare `shape.parse_json` para converter esse texto em uma coluna estruturada antes dos demais passos do `shape`.

```yaml
shape:
  parse_json:
    - column: payload
      schema: "STRUCT<customer: STRUCT<email: STRING, address: STRUCT<city: STRING>>, items: ARRAY<STRUCT<sku: STRING, qty: BIGINT>>>"
      alias: payload_json
      drop_source: false
  arrays:
    - path: payload_json.items
      mode: explode_outer
      alias: item
  columns:
    payload_json.customer.email:
      alias: customer_email
    payload_json.customer.address.city:
      alias: customer_city
    item.sku:
      alias: item_sku
```

Comportamento:

- `parse_json` só executa quando `shape` é declarado; fontes sem `shape` continuam intactas.
- Cada item de `parse_json` exige `column` e `schema`; o schema usa DDL Spark aceito por `from_json`.
- A coluna informada em `column` precisa ser `string`; se já for `struct`/`array`, remova `parse_json` e use os paths diretamente.
- Sem `alias`, a própria coluna string é sobrescrita pelo struct/array parseado.
- Com `alias`, a coluna original é preservada por padrão; use `drop_source: true` para removê-la.
- `drop_source: true` só é aceito para coluna top-level; em path aninhado, preserve a origem ou remova em etapa explícita posterior.
- JSON inválido ou incompatível com o schema segue a semântica do `from_json`: o resultado parseado fica nulo. Para tratar isso como erro de negócio, adicione `quality_rules.expressions` sobre os campos extraídos.

`shape.parse_json` não faz inferência automática por amostragem. Essa decisão mantém o contrato determinístico, evita ações extras no Spark e impede que mudanças ocasionais de payload alterem o schema de produção sem revisão.

### 5E.3 Flatten de Structs

```yaml
preset: silver_scd1_upsert
source: bronze.raw_orders
target_table: s_orders
catalog: main
merge_keys: order_id

shape:
  flatten:
    enabled: true
    separator: "_"
    include:
      - customer
      - shipping_address
    exclude:
      - customer.raw_document
    max_depth: 5
```

Exemplo:

```text
customer.email        -> customer_email
customer.address.city -> customer_address_city
```

### 5E.4 Extração de Paths com Alias

```yaml
shape:
  columns:
    customer.email:
      alias: customer_email
      cast: STRING
    customer.document.number: customer_document_number
    event_time:
      expression: "CAST(event_time_epoch_ms / 1000 AS TIMESTAMP)"
      alias: event_time
```

Essas colunas passam a existir antes de `quality_rules`, `merge_keys`, `hash_keys` e escrita.

Quando `shape.columns` é declarado, ele atua como **projeção declarativa**: a saída de negócio contém apenas os aliases declarados em `shape.columns` mais as colunas técnicas adicionadas pelo framework. Isso evita carregar, por acidente, colunas brutas ou metadados técnicos de uma camada anterior para a próxima. Para preservar uma coluna, declare-a explicitamente:

```yaml
shape:
  columns:
    order_id: order_id
    customer.email: customer_email
    event_ts:
      expression: "TO_TIMESTAMP(event_epoch_ms / 1000)"
      alias: event_ts
```

Colunas técnicas gerenciadas pelo framework (`ingestion_date`, `ingestion_ts_utc`, `source_system`, `__run_id`, `row_hash`, campos SCD etc.) são removidas automaticamente antes de serem recriadas na execução atual. Se a origem tiver uma coluna de negócio com nome reservado, preserve-a antes com `column_mapping` para um nome não reservado.

`shape.columns` aceita três formas:

- `path: alias` para extração direta.
- `path: {alias, cast}` para extração com cast Spark.
- `name: {expression, alias, cast}` para derivação simples com SQL Spark.

Use `expression` para normalizações pequenas diretamente ligadas ao shape, como conversão de epoch, indexação de array ou casts compostos. Transformações de negócio maiores continuam pertencendo ao notebook/job antes de chamar `ingest()`.

### 5E.5 Arrays e Arrays de Structs

Modos suportados:

| Modo | Cardinalidade | Resultado |
|------|---------------|-----------|
| `keep` | mantém | não altera a coluna |
| `to_json` | mantém | serializa array para string JSON |
| `size` | mantém | cria coluna com tamanho do array |
| `first` | mantém | cria coluna com primeiro elemento |
| `explode` | muda | uma linha por elemento, descartando arrays vazios |
| `explode_outer` | muda | uma linha por elemento, preservando arrays vazios/nulos |

Arrays aninhados podem ser declarados em qualquer ordem. A lib resolve dependências por path e alias:

```yaml
shape:
  arrays:
    - path: item.discounts
      mode: explode_outer
      alias: discount
    - path: items
      mode: explode_outer
      alias: item
  columns:
    order_id: order_id
    item.sku: item_sku
    discount.code: discount_code
```

Fluxo efetivo:

```text
items[]          -> item
item.discounts[] -> discount
item.sku         -> item_sku
discount.code    -> discount_code
```

### 5E.6 Arrays Paralelos com `zip_arrays`

APIs como Open-Meteo retornam arrays paralelos no mesmo struct: `hourly.time[]`, `hourly.temperature_2m[]`, `hourly.relative_humidity_2m[]`. Fazer `explode` em cada array separadamente geraria produto cartesiano. Para esse caso, declare primeiro `zip_arrays`, gerando um array de structs alinhado por índice, e depois faça `explode_outer` por `shape.arrays`.

```yaml
shape:
  zip_arrays:
    - alias: hourly_rows
      columns:
        hourly.time: time
        hourly.temperature_2m: temperature_2m
        hourly.relative_humidity_2m: relative_humidity_2m
        hourly.precipitation_probability: precipitation_probability
  arrays:
    - path: hourly_rows
      mode: explode_outer
      alias: hour
  columns:
    hour.time: forecast_hour
    hour.temperature_2m: temperature_2m
    hour.relative_humidity_2m: relative_humidity_2m
    hour.precipitation_probability: precipitation_probability
```

Comportamento:

- `zip_arrays[].columns` exige pelo menos dois arrays.
- Todos os paths declarados precisam existir e ser `array`.
- O alias de `zip_arrays` cria uma coluna `array<struct<...>>`.
- O alinhamento segue a semântica de `arrays_zip` do Spark: arrays de tamanhos diferentes são alinhados por posição e valores ausentes viram `null`.
- A mudança de cardinalidade só acontece quando `shape.arrays` usa `explode`/`explode_outer`, portanto os guardrails de Bronze continuam valendo.
- Aliases técnicos consumidos pelo próprio `shape` são removidos automaticamente. No exemplo acima, `hourly_rows` e `hour` não ficam na tabela final se serviram apenas para alimentar `shape.arrays` e `shape.columns`.

### 5E.7 Guardrails de Cardinalidade

Em Bronze, `explode` e `explode_outer` falham por padrão:

```yaml
shape:
  arrays:
    - path: items
      mode: explode_outer
      alias: item
```

Erro esperado: mudança de cardinalidade bloqueada em Bronze. Para permitir explicitamente:

```yaml
shape:
  allow_cardinality_change_on_bronze: true
  arrays:
    - path: items
      mode: explode_outer
      alias: item
```

Arrays irmãos com explode podem gerar produto cartesiano:

```yaml
shape:
  arrays:
    - path: items
      mode: explode_outer
      alias: item
    - path: payments
      mode: explode_outer
      alias: payment
```

Se `items` tem 2 elementos e `payments` tem 2 elementos, o resultado pode ter 4 linhas. A lib bloqueia esse caso por padrão. Para confirmar intencionalmente:

```yaml
shape:
  arrays:
    - path: items
      mode: explode_outer
      alias: item
    - path: payments
      mode: explode_outer
      alias: payment
      allow_cartesian: true
```

### 5E.8 Exemplo Completo Silver

```yaml
preset:
  - silver_scd1_upsert
  - quality_quarantine

source: bronze.raw_orders_json
target_table: s_order_items
catalog: main
merge_keys: order_item_key

shape:
  parse_json:
    - column: payload
      schema: "STRUCT<order_id: STRING, customer: STRUCT<email: STRING>, items: ARRAY<STRUCT<sku: STRING, quantity: BIGINT, discounts: ARRAY<STRUCT<code: STRING>>>>>"
      alias: payload_json
  arrays:
    - path: payload_json.items
      mode: explode_outer
      alias: item
    - path: item.discounts
      mode: to_json
      alias: item_discounts_json
  columns:
    payload_json.order_id: order_id
    payload_json.customer.email: customer_email
    item.sku: item_sku
    item.quantity: item_quantity
  flatten:
    enabled: true
    include: [payload_json]
    separator: "_"

custom_keys:
  order_item_key: order_id|item_sku

quality_rules:
  not_null: [order_id, item_sku]
  unique_key: [order_item_key]

annotations:
  columns:
    customer_email:
      description: "Email do cliente extraído do JSON."
      pii:
        enabled: true
        type: email
        sensitivity: restricted
    item_sku:
      description: "SKU do item do pedido."
```

---

## 6. Modos de Escrita — Guia Detalhado

### 6.1 Tabela Comparativa

| Modo | Estratégia SQL | Idempotência | Histórico | Chave Obrigatória | Colunas Técnicas |
|------|---------------|-------------|-----------|-------------------|-----------------|
| `scd0_append` | APPEND | ❌ | ❌ | Nenhuma | `ingestion_date`, `source_system`, `__run_id` |
| `scd0_overwrite` | OVERWRITE [+ replaceWhere] | ✅ | ❌ | Nenhuma | `ingestion_date`, `source_system`, `__run_id` |
| `scd1_upsert` | MERGE INTO | ✅ | ❌ | `merge_keys` | `ingestion_date`, `source_system`, `__run_id` |
| `scd1_hash_diff` | APPEND (diff por hash) | ✅ relativa | ❌ | `hash_keys` | `row_hash`, `ingestion_date`, `source_system`, `__run_id` |
| `scd2_historical` | MERGE INTO + staging | ✅ | ✅ | `merge_keys` | `row_hash`, `valid_from`, `valid_to`, `is_current`, `changed_columns` |
| `snapshot_soft_delete` | MERGE INTO (com delete lógico) | ✅ | ❌ | `merge_keys` | `row_hash`, `is_active`, `deleted_at` |

### 6.2 `scd0_append` — Append Imutável

**Quando usar:** Eventos, logs, fatos transacionais, cargas incrementais que nunca atualizam registros anteriores. É o modo padrão.

**Comportamento:**
- Insere todas as linhas sem comparar com o destino
- Não atualiza registros existentes
- Permite evolução de schema conforme `schema_policy`

**Python:**
```python
ingest(
    source="raw_orders",
    target_table="b_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    watermark_columns="updated_at",
    source_system="erp",
)
```

**YAML (`contracts/bronze/b_orders.yaml`):**
```yaml
source: raw_orders
target_table: b_orders
catalog: main
layer: bronze
mode: scd0_append
watermark_columns: updated_at
source_system: erp
notebook_name: bronze_orders
```

> Para idempotência relativa, combine com `watermark_columns` — execuções repetidas não duplicam dados já processados.

### 6.3 `scd0_overwrite` — Substituição Total ou Parcial

**Quando usar:** Tabelas de referência, snapshots pequenos, reprocessamentos controlados, fatos agregados por período.

**Comportamento:**
- Sem `partition_column` + `partition_value`: sobrescreve a tabela inteira
- Com `partition_column` + `partition_value`: usa `replaceWhere` para sobrescrever apenas a partição

**Overwrite total:**
```python
ingest(
    source=df_ref,
    target_table="c_status_pedido",
    catalog="main",
    layer="silver",
    mode="scd0_overwrite",
    schema_policy="strict",
)
```

**YAML (`contracts/silver/c_status_pedido.yaml`):**
```yaml
source: ref_status_pedido
target_table: c_status_pedido
catalog: main
layer: silver
mode: scd0_overwrite
schema_policy: strict
notebook_name: silver_status_pedido
```

**Overwrite por partição:**
```python
ingest(
    source=df_mes,
    target_table="faturamento_mensal",
    catalog="main",
    layer="gold",
    mode="scd0_overwrite",
    partition_column="competencia",
    partition_value="2026-05",  # substitui apenas esta partição
)
```

**YAML (`contracts/gold/faturamento_mensal.yaml`):**
```yaml
source: c_faturamento
target_table: faturamento_mensal
catalog: main
layer: gold
mode: scd0_overwrite
partition_column: competencia
partition_value: "{{dt}}"           # placeholder resolvido em runtime
schema_policy: strict
notebook_name: gold_faturamento
```

> ⚠️ Sem `partition_value`, o overwrite apaga **toda** a tabela. Use com cautela em tabelas grandes.

### 6.4 `scd1_upsert` — Estado Atual (SCD Tipo 1)

**Quando usar:** Manter o estado atual de uma entidade sem preservar histórico. É o modo mais comum em Silver.

**Comportamento:**
- Usa `MERGE INTO` com `t.key <=> s.key` (IS NOT DISTINCT FROM — trata `NULL = NULL` como verdade)
- Linhas com chave existente: **UPDATE** das colunas não-chave
- Linhas com chave nova: **INSERT**

**Python:**
```python
ingest(
    source="b_cliente",
    target_table="c_cliente",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
)
```

**YAML (`contracts/silver/c_cliente.yaml`):**
```yaml
source: b_cliente
target_table: c_cliente
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: id_cliente
dedup_order_expr: "updated_at DESC NULLS LAST"
schema_policy: additive_only
notebook_name: silver_cliente
```

**Estratégias de merge** (parâmetro `merge_strategy`, ver §10 para detalhes):
- `"delta"` (default) — MERGE puro, varre toda a tabela target
- `"delta_by_partition"` — adiciona predicado `IN (part_vals)` para reduzir arquivos varridos
- `"replace_partitions"` — OVERWRITE com `replaceWhere`, mais rápido quando source contém estado completo das partições

### 6.5 `scd1_hash_diff` — Append com Hash Diff

**Quando usar:** Origem não fornece CDC confiável, mas é necessário evitar inserir versões idênticas das mesmas chaves.

**Comportamento:**
1. Calcula `row_hash` (SHA-256) sobre todas as colunas de negócio (exclui `CONTROL_COLUMNS` e `hash_exclude_columns`)
2. Lê o target, extrai o "último estado" de cada `hash_key` (via `dedup_order_expr` ou heurística automática)
3. Faz LEFT JOIN + anti-join lógico: insere apenas linhas onde `row_hash` difere ou chave não existe
4. Append das diferenças

**Python:**
```python
ingest(
    source="b_produto",
    target_table="c_produto_versions",
    catalog="main",
    layer="silver",
    mode="scd1_hash_diff",
    hash_keys="id_produto",
    hash_exclude_columns="updated_at|source_file",  # colunas voláteis ignoradas no hash
    dedup_order_expr="updated_at DESC NULLS LAST",
    partition_column="ingestion_date",  # reduz leitura do target
)
```

**YAML (`contracts/silver/c_produto_versions.yaml`):**
```yaml
source: b_produto
target_table: c_produto_versions
catalog: main
layer: silver
mode: scd1_hash_diff
hash_keys: id_produto
hash_exclude_columns: updated_at|source_file
dedup_order_expr: "updated_at DESC NULLS LAST"
partition_column: ingestion_date
schema_policy: additive_only
notebook_name: silver_produto_hash
```

> **Como o framework determina o "último estado" sem `dedup_order_expr`:**
> 1. Tenta `ingestion_sequence DESC NULLS LAST` (se a coluna existe no target)
> 2. Tenta `ingestion_ts_utc DESC NULLS LAST, __run_id DESC NULLS LAST`
> 3. Se target tiver múltiplas versões por chave sem ordenação determinística → `ValueError`
>
> **Recomendação:** sempre informe `dedup_order_expr` explicitamente para evitar ambiguidade.

### 6.6 `scd2_historical` — Histórico Completo (SCD Tipo 2)

**Quando usar:** Preservar histórico completo de alterações de uma entidade (ex.: dimensions in data warehouse).

**Colunas geradas no target:**

| Coluna | Descrição |
|--------|-----------|
| `valid_from` | Início da validade (vem de `scd2_effective_from_column` ou `current_timestamp()`) |
| `valid_to` | Fim da validade (`NULL` = corrente, preenchido ao fechar versão) |
| `is_current` | `true` para a versão corrente, `false` para históricas |
| `row_hash` | Hash apenas das `scd2_change_columns` (mudanças fora delas não geram nova versão) |
| `changed_columns` | CSV das colunas que mudaram nesta transição |

**Comportamento:**
1. Calcula `row_hash` apenas sobre `scd2_change_columns` (ou todas exceto chaves + controle se vazio)
2. Compara com `is_current=true` do target
3. Para cada chave com mudança: fecha versão antiga (`valid_to=now`, `is_current=false`) e insere nova (`valid_from`, `is_current=true`)
4. Chaves reaparecidas (previamente inativas) geram nova versão corrente (não reativam a antiga)
5. Usa "staged rows" (duas variantes por linha changed) para forçar UPDATE + INSERT no mesmo MERGE

**Python:**
```python
ingest(
    source="c_cliente",
    target_table="dim_cliente_historico",
    catalog="main",
    layer="silver",
    mode="scd2_historical",
    merge_keys="id_cliente",
    scd2_change_columns="nome|email|status|cidade",  # só mudanças nessas colunas versionam
    scd2_effective_from_column="updated_at",
    cluster_columns="id_cliente|status",
    schema_policy="additive_only",
)
```

**YAML (`contracts/silver/dim_cliente_historico.yaml`):**
```yaml
source: c_cliente
target_table: dim_cliente_historico
catalog: main
layer: silver
mode: scd2_historical
merge_keys: id_cliente
scd2_change_columns: nome|email|status|cidade
scd2_effective_from_column: updated_at
cluster_columns: id_cliente|status
schema_policy: additive_only
notebook_name: silver_cliente_scd2
description: "Dimensão de cliente com histórico SCD2"
owner: data-platform
domain: comercial
```

> **Dica:** `scd2_change_columns` deve ser o conjunto **mínimo** que define uma "mudança real". Incluir colunas voláteis (timestamps de extração, etc.) gera versões desnecessárias.

### 6.7 `snapshot_soft_delete` — Snapshot com Soft Delete

**Quando usar:** A origem envia um snapshot completo do estado atual, e registros ausentes devem ser tratados como inativos (não deletados fisicamente).

Contrato semântico: o source precisa representar o estado final completo do domínio naquela execução. O modo não é incremental. Se o dado disponível é apenas o delta desde a última carga, use `scd1_upsert` ou `scd1_hash_diff`.

**Colunas geradas no target:**

| Coluna | Descrição |
|--------|-----------|
| `is_active` | `true` se presente no snapshot, `false` se ausente |
| `deleted_at` | Timestamp de quando foi marcado inativo |
| `row_hash` | Hash de todas as colunas de negócio |

**Comportamento:**
1. MERGE com `t.key <=> s.key`
2. MATCHED + hash diferente → UPDATE
3. NOT MATCHED → INSERT (nova)
4. **NOT MATCHED BY SOURCE + is_active=true → UPDATE SET is_active=false, deleted_at=now()** (soft delete)
5. MATCHED + is_active=false → UPDATE (re-ativa registros que voltaram a aparecer)

**Por que SQL MERGE:** o framework usa SQL `MERGE` em todos os runtimes para manter o mesmo comportamento em cluster classic, Databricks Serverless e Spark Connect. A decisão está registrada em [ADR-003](adrs/ADR-003-snapshot-soft-delete-sql-merge.md).

**Python:**
```python
ingest(
    source="snapshot_customers_today",
    target_table="c_customer_snapshot",
    catalog="main",
    layer="silver",
    mode="snapshot_soft_delete",
    merge_keys="customer_id",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
)
```

**YAML (`contracts/silver/c_customer_snapshot.yaml`):**
```yaml
source: snapshot_customers_today
target_table: c_customer_snapshot
catalog: main
layer: silver
mode: snapshot_soft_delete
merge_keys: customer_id
dedup_order_expr: "updated_at DESC NULLS LAST"
schema_policy: additive_only
notebook_name: silver_customer_snapshot
# watermark_columns NÃO pode ser usado com snapshot_soft_delete
# filter_expression NÃO pode ser usado com snapshot_soft_delete
```

> ⚠️ **Restrição crítica:** snapshot_soft_delete **NÃO aceita** `watermark_columns` nem `filter_expression`. O framework rejeita com `ValueError`. Um source filtrado faria todas as linhas fora do filtro virarem `is_active=false` erroneamente. Para sincronização incremental, use `scd1_upsert`.

Também não use Autoloader para esse modo. Autoloader `available_now` entrega micro-batches de arquivos novos; isso é carga incremental, não snapshot completo.

### 6.8 Restrições de Modo por Camada

| Modo | Bronze | Silver | Gold |
|------|--------|--------|------|
| `scd0_append` | ✅ | ✅ | ✅ |
| `scd0_overwrite` | ✅ | ✅ | ✅ |
| `scd1_upsert` | ❌ | ✅ | ✅ |
| `scd1_hash_diff` | ✅ | ✅ | ✅ |
| `scd2_historical` | ❌ | ✅ | ✅ |
| `snapshot_soft_delete` | ❌ | ✅ | ✅ |

---

## 7. Quality Gates — Guia Completo

### 7.1 Estrutura do `QualityRules`

```python
@dataclass(frozen=True)
class QualityRules:
    required_columns: List[str]       # Colunas que DEVEM existir
    not_null: List[str]               # Colunas que NÃO podem ter NULL
    unique_key: List[str]             # Conjunto de colunas que deve ser único
    accepted_values: Dict[str, List]  # Coluna → lista de valores permitidos
    min_rows: Optional[int]           # Mínimo de linhas após preparação
    max_null_ratio: Dict[str, float]  # Coluna → razão máxima de NULLs (0.0 a 1.0)
    expressions: List[QualityExpression]  # Expressões SQL booleanas customizadas
```

### 7.2 QualityExpression (Regras Customizadas)

```python
@dataclass(frozen=True)
class QualityExpression:
    name: str                              # Nome único da regra
    expression: str                        # Expressão SQL booleana
    severity: "warn" | "quarantine" | "abort" = "quarantine"
    message: Optional[str] = None          # Mensagem descritiva em falha
```

### 7.3 Avaliação (Single-Pass Aggregation)

Para reduzir I/O, o framework consolida regras de coluna (`not_null`, `accepted_values`, `max_null_ratio`) e `quality_rules.expressions` em uma única passagem `df.agg(...)`:

```python
agg_exprs = [count(*)]
for c in not_null:          agg_exprs.append(sum(col.isNull()))
for c in accepted_values:   agg_exprs.append(sum(~isin(vals) & isNotNull()))
for expr in expressions:    agg_exprs.append(sum(NOT (expr)))
# Uma única ação sobre o DataFrame
agg_row = df.agg(*agg_exprs).collect()[0]
```

**Exceções** (passagens próprias):
- `unique_key` — `groupBy(keys).count().where(count>1).count()`
- `required_columns` — só inspeção de schema, não toca dados

### 7.4 Ações em Falha (`on_quality_fail`)

| Ação | Comportamento | Quando usar |
|------|--------------|-------------|
| `"fail"` | Aborta a execução, `status=FAILED` | Padrão. Use quando dados inválidos são inaceitáveis |
| `"warn"` | Registra falhas, mas escreve tudo | Desenvolvimento, migração, ou quando a qualidade é informativa |
| `"quarantine"` | Linhas inválidas → `ctrl_ingestion_quarantine`; válidas → target | Quando você quer isolar problemas sem perder o resto |

### 7.5 Regras Abortivas (Abort-Only)

Três regras são **abort-only** — descrevem propriedades do conjunto e não conseguem isolar linhas individuais:

| Regra | Por que é abort-only |
|-------|---------------------|
| `unique_key` | Qual linha "fica" e qual "vai"? Decisão arbitrária sem reprocessamento |
| `required_columns` | A coluna inteira está faltando — não há linha a isolar |
| `min_rows` | Contagem mínima é propriedade agregada |

**Comportamento:** quando `on_quality_fail="quarantine"` e qualquer regra abort-only falha, o framework **escala automaticamente para `"fail"`** e aborta a execução. Isso evita o pior caso: escrever o dataset inteiro com `status=FAILED`.

> Para tolerar falhas de `unique_key`: use `dedup_order_expr` antes do quality gate. Para tolerar `min_rows`: use `on_quality_fail="warn"`.

### 7.6 Exemplo com Quality Gates via Dict

**Python:**
```python
ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id",
    quality_rules={
        "required_columns": ["order_id", "updated_at", "status"],
        "not_null": ["order_id", "updated_at"],
        "unique_key": ["order_id"],
        "accepted_values": {"status": ["open", "closed", "cancelled"]},
        "min_rows": 1,
        "max_null_ratio": {"customer_email": 0.20},
        "expressions": [
            {
                "name": "positive_amount",
                "expression": "amount > 0",
                "severity": "quarantine",
                "message": "Valor deve ser positivo.",
            },
            {
                "name": "valid_period",
                "expression": "end_date >= start_date",
                "severity": "abort",
                "message": "Período inválido — dados inconsistentes.",
            },
        ],
    },
    on_quality_fail="quarantine",
    # unique_key e required_columns são abort-only:
    # se falharem, a execução escala para fail mesmo com on_quality_fail="quarantine".
    # Para quarentena efetiva, use apenas not_null, accepted_values, max_null_ratio
    # e expressions com severity="quarantine".
)
```

**YAML (`contracts/silver/c_orders.yaml`):**
```yaml
source: b_orders
target_table: c_orders
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: order_id
quality_rules:
  required_columns: [order_id, updated_at, status]
  not_null: [order_id, updated_at]
  unique_key: [order_id]
  accepted_values:
    status: [open, closed, cancelled]
  min_rows: 1
  max_null_ratio:
    customer_email: 0.20
  expressions:
    - name: positive_amount
      expression: "amount > 0"
      severity: quarantine
      message: "Valor deve ser positivo."
    - name: valid_period
      expression: "end_date >= start_date"
      severity: abort
      message: "Período inválido — dados inconsistentes."
on_quality_fail: quarantine
# unique_key, required_columns e min_rows são abort-only:
# se falharem, a execução escala para fail mesmo com quarantine.
notebook_name: silver_orders
schema_policy: additive_only
```

### 7.7 Exemplo com QualityRules via Dataclass

```python
from contractforge import QualityRules, QualityExpression

rules = QualityRules(
    required_columns=["id_cliente", "updated_at"],
    not_null=["id_cliente"],
    unique_key=["id_cliente"],
    max_null_ratio={"email": 0.15},
    expressions=[
        QualityExpression(
            name="valid_email",
            expression="email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'",
            severity="quarantine",
            message="Email em formato inválido.",
        )
    ],
)

ingest(
    source="b_cliente",
    target_table="c_cliente",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    quality_rules=rules,
    on_quality_fail="fail",
)
```

### 7.8 Limite de `accepted_values`

O framework impõe um limite de **1000 valores** por coluna em `accepted_values` (configurável via `FrameworkConfig.max_inline_accepted_values`). Acima disso, `isin([...])` no Spark causa problemas de performance. A solução é usar uma tabela de referência + `LEFT ANTI JOIN` antes da chamada:

```python
# Em vez de accepted_values com 10k itens:
df_clean = df.join(
    spark.table("ref.tipos_validos"),
    on="tipo",
    how="leftsemi"  # mantém apenas os válidos
)
# Depois chama ingest() com o df_clean
```

### 7.9 Regras Customizadas via Registry

Para casos específicos, registre avaliadores com `register_quality_rule`. O avaliador recebe `(df, rule_name, config)` e retorna ao menos `failed_count`; pode retornar `message`, `details` e `condition` para regras quarentenáveis.

```python
from contractforge import register_quality_rule, ingest
from pyspark.sql import functions as F

def freshness_rule(df, rule_name, config):
    column = config["column"]
    max_age_days = int(config["max_age_days"])
    condition = F.col(column) < F.date_sub(F.current_date(), max_age_days)
    return {
        "failed_count": df.where(condition).count(),
        "condition": condition,
        "message": f"{column} excedeu {max_age_days} dias",
    }

register_quality_rule("freshness", freshness_rule)

ingest(
    source="b_events",
    target_table="c_events",
    mode="scd1_upsert",
    merge_keys="event_id",
    quality_rules={
        "custom": {
            "event_freshness": {
                "type": "freshness",
                "column": "event_date",
                "max_age_days": 7,
                "severity": "warn",
            }
        }
    },
)
```

---

## 8. Schema Policy — Evolução de Schema

### 8.1 Políticas

| Política | Novas Colunas | Colunas Removidas | Mudança de Tipo | Quando usar |
|----------|---------------|-------------------|-----------------|-------------|
| `"permissive"` | ✅ Aceita (adiciona via ALTER) | ✅ Aceita | ❌ Rejeita inseguras | Origens instáveis, fase de descoberta |
| `"additive_only"` | ✅ Aceita (adiciona via ALTER) | ❌ Rejeita | ❌ Rejeita inseguras | Silver/Gold com contratos que só crescem |
| `"strict"` | ❌ Rejeita | ❌ Rejeita | ❌ Rejeita | Tabelas de consumo com contrato fixo |

### 8.2 Tipo de Mudanças Bloqueadas vs Permitidas

**Sempre bloqueadas** (mudanças potencialmente destrutivas):
- `string → int`, `double → int`, `timestamp → date`, etc.

**Permitidas com `allow_type_widening=True`** (alargamentos seguros):
- `int → bigint`, `smallint → int`, `tinyint → smallint`
- `float → double`
- Aumento de precisão decimal (ex.: `decimal(10,2) → decimal(18,2)`)
- `date → timestamp`
- `int → double`

Quando `allow_type_widening=True` e uma mudança segura é detectada, o framework aplica `ALTER TABLE ALTER COLUMN TYPE` e registra em `ctrl_ingestion_schema_changes`.

### 8.3 Exemplos

```python
# Bronze: esquema de origem instável, aceitamos qualquer coisa
ingest(source="raw_events", target_table="b_events",
       layer="bronze", mode="scd0_append", schema_policy="permissive")

# Silver: contrato aditivo — colunas novas OK, mas nada de remover ou mudar tipo
ingest(source="b_cliente", target_table="c_cliente",
       layer="silver", mode="scd1_upsert", merge_keys="id",
       schema_policy="additive_only")

# Gold: tabela de consumo com schema fixo
ingest(source=df_agg, target_table="f_vendas_diario",
       layer="gold", mode="scd0_overwrite", schema_policy="strict")

# Com alargamento de tipo:
ingest(source=df, target_table="c_metricas",
       layer="silver", mode="scd1_upsert", merge_keys="id",
       schema_policy="additive_only", allow_type_widening=True)
```

**Exemplo YAML unificado de schema policy por camada:**

```yaml
# contracts/bronze/b_events.yaml — origem instável
source: raw_events
target_table: b_events
catalog: main
layer: bronze
mode: scd0_append
schema_policy: permissive        # aceita qualquer schema

---
# contracts/silver/c_cliente.yaml — contrato aditivo
source: b_cliente
target_table: c_cliente
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: id_cliente
schema_policy: additive_only      # só adiciona colunas

---
# contracts/gold/f_vendas_diario.yaml — schema fixo
source: c_vendas
target_table: f_vendas_diario
catalog: main
layer: gold
mode: scd0_overwrite
schema_policy: strict             # rejeita qualquer divergência

---
# contracts/silver/c_metricas.yaml — aditivo com alargamento
source: b_metricas
target_table: c_metricas
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: id
schema_policy: additive_only
allow_type_widening: true         # int→bigint, float→double, etc.
```

> **Nota:** `allow_type_widening=True` é incompatível com `schema_policy="strict"` (validado em construção do plan).

---

## 9. Watermarks — Carga Incremental

### 9.1 Conceito

Watermarks permitem que cada execução processe apenas dados **novos** (posteriores ao último processamento bem-sucedido). O framework persiste o watermark em `ctrl_ingestion_state` como JSON tipado, garantindo que:
- Comparações preservam tipos (`timestamp > timestamp`, `bigint > bigint`)
- O watermark **só avança** após execução com `status=SUCCESS`
- Em falha, o watermark anterior é mantido (não regride nem avança)

### 9.2 Watermark Simples (1 coluna)

**Python:**
```python
ingest(
    source="raw_orders",
    target_table="b_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    watermark_columns="updated_at",  # processa apenas linhas com updated_at > último watermark
)
```

**YAML (`contracts/bronze/b_orders.yaml`):**
```yaml
source: raw_orders
target_table: b_orders
catalog: main
layer: bronze
mode: scd0_append
watermark_columns: updated_at
```

Filtro gerado: `WHERE updated_at > '2024-01-15 12:30:00'`

### 9.3 Watermark Composto (múltiplas colunas)

**Python:**
```python
ingest(
    source="raw_movimentos",
    target_table="b_movimentos",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    watermark_columns="data_movimento|hora_movimento|seq_movimento",
)
```

**YAML (`contracts/bronze/b_movimentos.yaml`):**
```yaml
source: raw_movimentos
target_table: b_movimentos
catalog: main
layer: bronze
mode: scd0_append
watermark_columns: data_movimento|hora_movimento|seq_movimento
```

Filtro gerado (comparação lexicográfica):
```sql
WHERE (data_movimento > L1)
   OR (data_movimento = L1 AND hora_movimento > L2)
   OR (data_movimento = L1 AND hora_movimento = L2 AND seq_movimento > L3)
```

### 9.4 Encoding do Watermark

Watermarks são serializados como JSON com tipo e valor:

```json
{
  "updated_at": {"type": "timestamp", "value": "2024-01-15 12:30:00"},
  "version":    {"type": "bigint", "value": "42"}
}
```

### 9.5 Fallback em Cascata

Quando `ctrl_ingestion_state` não tem watermark (primeira execução ou perda da state table), o framework tenta:
1. `SELECT MAX(col) FROM target_table` diretamente
2. Se o target não existe → `None` (processa tudo)

### 9.6 Troubleshooting de Watermark

| Sintoma | Causa | Ação |
|---------|-------|------|
| Watermark não avança | Falha na execução, coluna com NULLs, ou sem dados novos | Verifique `ctrl_ingestion_state.watermark_value` e logs de erro |
| Dados duplicados | Coluna de watermark não é monótona | Use `dedup_order_expr` ou `unique_key` nos quality gates |
| "Watermark não contém as colunas esperadas" | Mudou `watermark_columns` entre execuções | Limpe a state table ou use uma nova `target_table` |

---

## 10. Estratégias de Merge (scd1_upsert)

O parâmetro `merge_strategy` controla como o `MERGE` é executado em `scd1_upsert`:

### 10.1 `"delta"` (default)

MERGE puro com `t.key <=> s.key` (IS NOT DISTINCT FROM). Varre toda a tabela target.

**Python:**
```python
ingest(source=df, target_table="c_cliente", mode="scd1_upsert",
       merge_keys="id_cliente", merge_strategy="delta")
```

**YAML:**
```yaml
merge_strategy: delta   # default, pode ser omitido
```

**Quando usar:** Tabelas pequenas/médias, ou quando não há partição relevante para filtrar.

### 10.2 `"delta_by_partition"`

Adiciona predicado `AND t.partition_col IN (valores_afetados)` na cláusula `ON`, reduzindo arquivos varridos.

**Python:**
```python
ingest(source=df, target_table="c_vendas", mode="scd1_upsert",
       merge_keys="id_venda",
       merge_strategy="delta_by_partition",
       merge_partition_column="dt_venda")
```

**YAML (`contracts/silver/c_vendas.yaml`):**
```yaml
source: b_vendas
target_table: c_vendas
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: id_venda
merge_strategy: delta_by_partition
merge_partition_column: dt_venda
```

**Quando usar:** Tabelas grandes particionadas, quando o source toca poucas partições.

### 10.3 `"replace_partitions"`

Não faz MERGE — faz **OVERWRITE** com `replaceWhere = partition_col IN (vals)`. Assume que o source contém o **estado completo** das partições afetadas.

**Python:**
```python
ingest(source=df, target_table="f_vendas_diario", mode="scd1_upsert",
       merge_keys="id_venda",
       merge_strategy="replace_partitions",
       merge_partition_column="dt_referencia",
       replace_partitions_source_complete=True)
```

**YAML (`contracts/gold/f_vendas_diario.yaml`):**
```yaml
source: c_vendas
target_table: f_vendas_diario
catalog: main
layer: gold
mode: scd1_upsert
merge_keys: id_venda
merge_strategy: replace_partitions
merge_partition_column: dt_referencia
replace_partitions_source_complete: true   # confirmação explícita obrigatória
```

**Quando usar:** Refeitura diária por partição onde o source tem o estado-fim completo daquela partição.

> ⚠️ **Exigências:**
> - `replace_partitions_source_complete=True` é obrigatório (confirmação explícita do usuário)
> - `merge_partition_column` é obrigatório
> - Se `partition_column` também for informado, deve ser igual a `merge_partition_column`
> - Linhas que existem no target mas não no source **serão perdidas** nas partições afetadas

### 10.4 Performance Relativa

| Estratégia | Velocidade | Custo | Risco |
|------------|-----------|-------|-------|
| `delta` | Base | Base | Nenhum |
| `delta_by_partition` | Mais rápida | Menor | Nenhum (só reduz escopo) |
| `replace_partitions` | Mais rápida | Menor | Perda de dados se source incompleto |

---

## 11. Locks, Idempotência, Retry e Concorrência

### 11.1 Lock Operacional (`lock_enabled`)

Lock best-effort por `target_table` usando `ctrl_ingestion_locks`:

**Python:**
```python
ingest(source="b_cliente", target_table="c_cliente",
       mode="scd1_upsert", merge_keys="id_cliente",
       lock_enabled=True)
```

**YAML:**
```yaml
lock_enabled: true
```

**Funcionamento:**
- Antes da escrita, faz MERGE em `ctrl_ingestion_locks` e lê de volta para confirmar que este `run_id` ficou como `ACTIVE`
- Locks expirados (TTL padrão: 120 min) são rompidos automaticamente
- No `finally`, o lock é liberado (`status=RELEASED`)
- **Best-effort:** há janela de corrida entre MERGE e read-back

> O lock **não substitui** o controle otimista de concorrência do Delta Lake. Use para reduzir colisões previsíveis. A consistência final continua baseada no Delta.

### 11.2 Retry para Conflitos Delta

O framework automaticamente retenta operações de escrita que falham com erros de concorrência Delta:

```python
# Configurável via FrameworkConfig (default: 3 tentativas, backoff 5s linear + jitter)
with_retry(lambda: execute_write_mode(...))
```

**Erros que disparam retry:** `CONCURRENT`, `CONFLICT`, `RETRY`, `DELTA_CONCURRENT`

**Erros que NÃO disparam retry** (propagam imediatamente): OOM, schema mismatch, permissão, etc.

Durante `ensure_ctrl_tables`, o registro em `ctrl_ingestion_metadata` é tratado de forma idempotente por versão. Se múltiplas tasks iniciarem em paralelo com o mesmo `ctrl_schema` e outra execução já tiver gravado a mesma `framework_version`/`ctrl_schema_version`, conflitos Delta nessa escrita de metadata não interrompem a ingestão.

### 11.3 Idempotência (`idempotency_key` + `idempotency_policy`)

Permite identificar unicamente um lote lógico e controlar reexecuções:

| `idempotency_policy` | Comportamento |
|---------------------|---------------|
| `"always_run"` | Sempre executa (default) |
| `"skip_if_success"` | Se já existe `SUCCESS` para esta `idempotency_key`, retorna `status="SKIPPED"` |
| `"rerun_if_failed"` | Se já existe `SUCCESS`, pula. Se último status foi `FAILED`, reexecuta |
| `"fail_if_success"` | Se já existe `SUCCESS`, **aborta com erro** (proteção contra dupla execução) |

**Python:**
```python
ingest(
    source="b_pedidos",
    target_table="c_pedidos",
    mode="scd1_upsert",
    merge_keys="id_pedido",
    idempotency_key="job-42:batch-2026-05-11",
    idempotency_policy="skip_if_success",
)
```

**YAML (`contracts/silver/c_pedidos.yaml`):**
```yaml
source: b_pedidos
target_table: c_pedidos
mode: scd1_upsert
merge_keys: id_pedido
idempotency_key: "job-42:batch-2026-05-11"
idempotency_policy: skip_if_success
```

> `idempotency_policy != "always_run"` exige `idempotency_key` (validado em construção do plan).

---

## 12. Observabilidade — Tabelas de Controle

As tabelas de controle são criadas automaticamente no schema `ctrl_schema` (default `ops`):

### 12.1 `ctrl_ingestion_runs`

Histórico completo de todas as execuções. Particionada por `run_date`.

**Colunas principais:** `run_id`, `run_ts_utc`, `run_date`, `notebook_name`, `layer`, `source_table`, `source_type`, `source_connector`, `source_name`, `source_provider`, `source_format`, `source_path`, `source_options_json`, `source_read_json`, `source_request_json`, `source_auth_json`, `source_pagination_json`, `source_response_json`, `source_incremental_json`, `source_limits_json`, `source_capabilities_json`, `source_metrics_json`, `target_table`, `mode`, `status`, `rows_read`, `rows_written`, `rows_inserted`, `rows_updated`, `rows_deleted`, `rows_quarantined`, `watermark_previous`, `watermark_current`, `duration_seconds`, `quality_status`, `schema_policy`, `schema_changes_json`, `stage_durations_json`, `operation_metrics_json`, `write_committed`, `delta_version_before`, `delta_version_after`, `error_message`, `idempotency_key`, `idempotency_policy`, `skip_reason`, `skipped_by_run_id`, `contract_description`, `contract_owner`, `contract_domain`, `contract_tags_json`, `contract_sla`, `runtime_parameters_json`, `metrics_source`, `framework_version`, `ctrl_schema_version`, `runtime_type`, `spark_version`, `python_version`.

### 12.2 `ctrl_ingestion_state`

Uma linha por `target_table` — sempre o estado mais recente.

**Colunas:** `target_table` (PK), `watermark_column`, `watermark_value`, `last_success_at_utc`, `last_run_id`, `last_status`, `last_rows_written`, `last_error_message`, `last_delta_version`, `last_write_completed_at_utc`, `last_watermark_candidate`, `last_updated_at_utc`, `parent_run_id`, `run_group_id`, `master_job_id`, `master_run_id`.

### 12.3 `ctrl_ingestion_quality`

Uma linha por regra que falhou, por execução.

**Colunas:** `run_id`, `target_table`, `rule_name`, `status`, `severity`, `failed_count`, `checked_at_utc`, `message`, `details_json`.

### 12.4 `ctrl_ingestion_quarantine`

Linhas isoladas quando `on_quality_fail="quarantine"`.

**Colunas:** `run_id`, `target_table`, `rule_name`, `error_reason`, `record_payload` (JSON da linha original), `quarantined_at_utc`.

### 12.5 `ctrl_ingestion_errors`

Stack traces completos de execuções com falha.

**Colunas:** `run_id`, `error_ts_utc`, `error_date` (partição), `target_table`, `source_table`, `mode`, `status`, `error_type`, `error_message`, `stack_trace`, `framework_version`, `ctrl_schema_version`, `runtime_type`, `spark_version`, `python_version`.

### 12.6 `ctrl_ingestion_locks`

Reserva operacional best-effort por `target_table`.

**Colunas:** `target_table` (PK), `run_id`, `owner`, `acquired_at_utc`, `expires_at_utc`, `ttl_minutes`, `released_at_utc`, `status` (`ACTIVE`/`RELEASED`).

### 12.7 `ctrl_ingestion_explain`

Planos Spark capturados com `explain_mode=True`.

**Colunas:** `run_id`, `target_table`, `source_table`, `mode`, `explain_format`, `plan_text`, `captured_at_utc`.

### 12.8 `ctrl_ingestion_lineage`

Eventos OpenLineage em JSON.

**Colunas:** `run_id`, `event_time_utc`, `event_type`, `target_table`, `source_table`, `namespace`, `producer`, `event_json`.

### 12.9 `ctrl_ingestion_metadata`

Uma linha por componente. Registra `framework_version`, `ctrl_schema_version` e `updated_at_utc`. A tabela é atualizada apenas quando a versão atual ainda não está registrada, evitando conflitos desnecessários em jobs multi-task paralelos.

### 12.10 `ctrl_ingestion_schema_changes`

Histórico de evolução estrutural (adições de colunas, mudanças de tipo).

**Colunas:** `run_id`, `change_ts_utc`, `target_table`, `change_type` (`add_column`/`type_change`/`type_widening`), `column_name`, `source_type`, `target_type`, `applied`, `details_json`, `framework_version`, `ctrl_schema_version`.

### 12.11 `ctrl_ingestion_streams`

Histórico das execuções externas de Autoloader `available_now`.

**Colunas:** `stream_run_id`, `idempotency_key`, `idempotency_policy`, `skip_reason`, `skipped_by_stream_run_id`, `target_table`, `target_catalog`, `target_layer`, `notebook_name`, `source_type`, `source_path`, `trigger`, `checkpoint_location`, `status`, `started_at_utc`, `ended_at_utc`, `duration_seconds`, `batches_processed`, `total_rows_read`, `total_rows_written`, `total_rows_quarantined`, `framework_version`, `ctrl_schema_version`, `runtime_type`, `spark_version`, `python_version`, `error_message`, `master_job_id`, `master_run_id`, `parent_run_id`, `run_group_id`.

### 12.12 `ctrl_ingestion_annotations`

Auditoria de comments/tags aplicados por `annotations`.

**Colunas:** `run_id`, `target_table`, `annotation_scope`, `annotation_type`, `column_name`, `key`, `previous_value`, `value`, `status`, `error_message`, `applied_sql`, `annotation_ts_utc`, `annotation_date`, `framework_version`, `ctrl_schema_version`.

### 12.13 `ctrl_ingestion_operations`

Registro de criticidade, SLA, donos, grupos e runbook declarados em `operations`.

**Colunas:** `run_id`, `target_table`, `criticality`, `expected_frequency`, `freshness_sla_minutes`, `alert_on_failure`, `alert_on_quality_fail`, `runbook_url`, `ownership_json`, `owners_json`, `groups_json`, `tags_json`, `status`, `recorded_at_utc`, `framework_version`, `ctrl_schema_version`.

### 12.14 `ctrl_ingestion_access`

Auditoria de grants, row filters e column masks aplicados por `access`.

**Colunas:** `access_run_id`, `run_id`, `target_table`, `access_type`, `principal`, `privilege`, `column_name`, `function_name`, `object_name`, `status`, `error_message`, `applied_sql`, `previous_value`, `new_value`, `mode`, `drift_policy`, `revoke_unmanaged`, `access_ts_utc`, `access_date`, `framework_version`, `ctrl_schema_version`.

### 12.15 Consultas Úteis

```sql
-- Últimas execuções por tabela
SELECT target_table, status, rows_written, duration_seconds, started_at_utc
FROM ops.ctrl_ingestion_runs
WHERE run_date = current_date()
ORDER BY started_at_utc DESC;

-- Estado atual de cada tabela
SELECT target_table, last_status, watermark_value, last_delta_version, last_success_at_utc
FROM ops.ctrl_ingestion_state
ORDER BY last_updated_at_utc DESC;

-- Falhas de qualidade recentes
SELECT q.target_table, q.rule_name, q.failed_count, q.message, q.details_json
FROM ops.ctrl_ingestion_quality q
JOIN ops.ctrl_ingestion_runs r USING (run_id)
WHERE r.run_date >= current_date() - 7
ORDER BY q.failed_count DESC;

-- Limpeza de dados antigos
DELETE FROM ops.ctrl_ingestion_runs WHERE run_date < current_date() - 90;
VACUUM ops.ctrl_ingestion_runs RETAIN 168 HOURS;

-- Streams Autoloader recentes
SELECT stream_run_id, target_table, source_path, status, batches_processed, total_rows_written
FROM ops.ctrl_ingestion_streams
ORDER BY started_at_utc DESC;

-- Annotations com falha ou warning
SELECT target_table, annotation_scope, annotation_type, column_name, status, error_message
FROM ops.ctrl_ingestion_annotations
WHERE status IN ('FAILED', 'WARNED')
ORDER BY annotation_ts_utc DESC;

-- Grants e politicas aplicadas
SELECT target_table, access_type, principal, privilege, object_name, status
FROM ops.ctrl_ingestion_access
ORDER BY access_ts_utc DESC;
```

---

## 13. OpenLineage e Explain Mode

### 13.1 OpenLineage

Quando `openlineage_enabled=True`, o framework gera um evento OpenLineage 1.0.5 e persiste em `ctrl_ingestion_lineage`.

**Facets incluídos no evento:**
- `processing_engine` — engine=spark + version
- `parent` — se `parent_run_id` informado
- `sourceCodeLocation` — type=notebook, url=notebook_name
- `schema` — colunas do input e output
- `dataQualityMetrics` — rowCount do output
- `contractforge` (custom) — mode, layer, rowsRead, rowsWritten, deltaVersionBefore/After, operationMetrics, started/finishedAt

```python
ingest(
    source="b_orders", target_table="c_orders",
    mode="scd1_upsert", merge_keys="order_id",
    openlineage_enabled=True,
    openlineage_namespace="databricks://main",  # opcional, default: databricks://<catalog>
    openlineage_producer="contractforge",
)
```

> Para enviar eventos a um collector externo (Marquez, OpenLineage proxy), crie um forwarder que leia `ctrl_ingestion_lineage` e faça POST HTTP.

### 13.2 Explain Mode

Captura o plano de execução Spark do DataFrame preparado:

```python
ingest(
    source="b_movimentos", target_table="c_movimentos",
    mode="scd1_upsert", merge_keys="id_movimento",
    explain_mode=True,
    explain_format="formatted",  # simple, extended, formatted, cost, codegen
)
```

**Consulta:**
```sql
SELECT run_id, explain_format, plan_text
FROM ops.ctrl_ingestion_explain
WHERE target_table = 'main.silver.c_movimentos'
ORDER BY captured_at_utc DESC;
```

> O explain é caro em DataFrames grandes — use apenas em desenvolvimento ou diagnóstico. O texto é truncado em 100.000 caracteres.

---

## 14. Linhagem Operacional (parent/master)

Os parâmetros `parent_run_id`, `run_group_id`, `master_job_id` e `master_run_id` são **puramente informativos** — não alteram o comportamento da ingestão, mas são propagados para `ctrl_ingestion_runs` e `ctrl_ingestion_state`, permitindo correlação com orquestradores externos.

```python
# Em um notebook de Databricks Workflow:
ingest(
    source="b_items", target_table="c_items",
    mode="scd1_upsert", merge_keys="item_id",
    parent_run_id=dbutils.widgets.get("parent_run_id"),
    run_group_id=dbutils.widgets.get("run_group_id"),
    master_job_id=dbutils.widgets.get("job_id"),
    master_run_id=dbutils.widgets.get("run_id"),
)
```

**Uso em dashboards:**
```sql
-- Todas as execuções de um job run específico
SELECT * FROM ops.ctrl_ingestion_runs
WHERE master_run_id = '12345'
ORDER BY started_at_utc;
```

---

## 15. Metadados de Contrato

Parâmetros que documentam o pipeline sem afetar a execução:

```python
ingest(
    source="b_cliente", target_table="c_cliente",
    mode="scd1_upsert", merge_keys="id_cliente",
    description="Clientes consolidados do CRM com deduplicação por updated_at",
    owner="data-platform",
    domain="comercial",
    tags=["silver", "cliente", "crm"],
    sla="D+0 08:00",
    runtime_parameters={"carga": "incremental", "prioridade": "alta"},
)
```

Esses valores são propagados no retorno (`contract_metadata`) e em `ctrl_ingestion_runs` (colunas `contract_description`, `contract_owner`, `contract_domain`, `contract_tags_json`, `contract_sla`, `runtime_parameters_json`).

### 15.1 Contratos Separados: ingestion, annotations, operations e access

Para tabelas com governança mais forte, o contrato pode ser dividido por responsabilidade:

```text
contracts/gold/gd_orders.ingestion.yaml
contracts/gold/gd_orders.annotations.yaml
contracts/gold/gd_orders.operations.yaml
contracts/gold/gd_orders.access.yaml
```

Carregamento e execução:

```python
from contractforge import ingest_bundle, load_contract_bundle

bundle = load_contract_bundle("contracts/gold/gd_orders")
result = ingest_bundle("contracts/gold/gd_orders")
```

Validação local sem Spark:

```bash
contractforge validate-bundle contracts/gold/gd_orders
contractforge validate-project contracts
contractforge init --output contracts/silver/c_orders --source bronze.b_orders --target-table c_orders --layer silver --mode scd1_upsert --merge-keys order_id --split
contractforge governance-preview contracts/gold/gd_orders
contractforge governance-check contracts/gold/gd_orders
contractforge drift-check contracts/gold/gd_orders
contractforge governance-apply contracts/gold/gd_orders
contractforge apply-annotations contracts/gold/gd_orders
contractforge validate-access contracts/gold/gd_orders
contractforge apply-access contracts/gold/gd_orders
contractforge apply-access contracts/gold/gd_orders --force-revoke
```

`annotations` aplica metadata técnica no catálogo:

```yaml
target:
  catalog: main
  schema: gold
  table: gd_orders
policy: warn
table:
  description: "Pedidos diários consolidados."
  aliases: [orders, sales_orders]
  tags:
    domain: sales
    data_product: orders
columns:
  customer_email:
    description: "Email do cliente."
    pii:
      enabled: true
      type: email
      sensitivity: restricted
    tags:
      confidentiality: restricted
```

`operations` registra contexto operacional em `ctrl_ingestion_operations` para dashboards/alertas externos:

```yaml
target:
  catalog: main
  schema: gold
  table: gd_orders
ownership:
  business_owner: sales-analytics
  technical_owner: data-platform
  steward: data-governance
  support_group: data-sre
  escalation_group: data-oncall
operations:
  criticality: high
  expected_frequency: daily
  freshness_sla_minutes: 180
  alert_on_failure: true
  alert_on_quality_fail: true
  runbook_url: "https://wiki/runbooks/gd_orders"
  tags:
    cost_center: analytics
```

`access` aplica governança de acesso:

```yaml
target:
  catalog: main
  schema: gold
  table: gd_orders
access_policy:
  mode: apply
  on_drift: warn
  revoke_unmanaged: false
grants:
  - principal: data-readers
    privileges: [SELECT]
row_filters:
  - name: filter_by_region
    function: main.security.fn_filter_region
    columns: [region]
column_masks:
  customer_email:
    function: main.security.mask_email
    using_columns: [customer_email]
```

Auditoria gerada:

- `ctrl_ingestion_annotations`: comments/tags aplicados, ignorados ou com warning/falha.
- `ctrl_ingestion_operations`: criticidade, SLA, donos, grupos, runbook e tags operacionais.
- `ctrl_ingestion_access`: grants, row filters e column masks aplicados/validados.

Falhas em annotations seguem `annotations.policy` (`fail`, `warn`, `ignore`). Falhas em access seguem `access_policy.mode` (`apply`, `validate_only`, `ignore`) e `access_policy.on_drift` (`fail`, `warn`, `reconcile`). O formato legado com `mode`/`on_drift` no topo de `access` também é aceito.

Para grants, o framework compara o declarado com `SHOW GRANTS ON TABLE`. O relatório aparece em `governance-check`/`drift-check` e em `governance.access.drift` no retorno. Se `revoke_unmanaged=true`, grants atuais não declarados só são revogados por `contractforge apply-access --force-revoke`; ingestão normal não aplica access e aplicação sem essa flag falha com mensagem explícita.

Semântica de `access_policy.on_drift`:

- `fail`: qualquer drift detectado em grants retorna `FAILED` em `validate-access`/`governance-check` e impede `apply-access` antes de executar SQL.
- `warn`: drift retorna `WARNED`, mas `apply-access` pode aplicar grants declarados ausentes; não remove grants não declarados.
- `reconcile`: permite reconciliar grants declarados ausentes e, se `revoke_unmanaged=true` com `--force-revoke`, remove grants não declarados.

`ingest_plan` aplica `operations` e `annotations` depois da escrita, mas deixa `access` como `DEFERRED`. A separação é intencional: permissões, masks e row filters normalmente exigem credenciais mais elevadas e devem rodar em pipeline dedicado de governança.

O framework também valida capabilities básicas de Unity Catalog antes de aplicar recursos de catálogo. Tags, row filters e column masks exigem alvo qualificado em três partes (`catalog.schema.table`); caso contrário, o contrato falha ou gera warning conforme a política declarada.

---

## 16. FrameworkConfig — Configuração Global

Dataclass frozen com defaults globais. A instância singleton é `contractforge.config.CONFIG`.

```python
@dataclass(frozen=True)
class FrameworkConfig:
    default_catalog: str = "main"
    default_source_system: str = "default"
    default_partition_col: str = "ingestion_date"
    ctrl_schema: str = "ops"
    ctrl_table_runs: str = "ctrl_ingestion_runs"
    ctrl_table_state: str = "ctrl_ingestion_state"
    ctrl_table_quality: str = "ctrl_ingestion_quality"
    ctrl_table_quarantine: str = "ctrl_ingestion_quarantine"
    ctrl_table_locks: str = "ctrl_ingestion_locks"
    ctrl_table_explain: str = "ctrl_ingestion_explain"
    ctrl_table_lineage: str = "ctrl_ingestion_lineage"
    ctrl_table_metadata: str = "ctrl_ingestion_metadata"
    ctrl_table_errors: str = "ctrl_ingestion_errors"
    ctrl_table_schema_changes: str = "ctrl_ingestion_schema_changes"
    ctrl_table_streams: str = "ctrl_ingestion_streams"
    max_error_len: int = 8000
    default_lock_ttl_minutes: int = 120
    default_retry_attempts: int = 3
    default_retry_backoff_seconds: int = 5
    max_inline_accepted_values: int = 1000
    max_partition_predicate_values: int = 1000
```

**Customização (monkey-patch, use com cautela):**
```python
import contractforge.config as cfg
cfg.CONFIG = cfg.FrameworkConfig(ctrl_schema="my_ops", default_retry_attempts=5)
```

> Prefira passar valores no `IngestionPlan` (ex.: `ctrl_schema="my_ops"`) em vez de alterar o CONFIG global.

---

## 16B. Extensões Programáticas

### 16B.1 Hooks de Ingestão

`IngestionHooks` permite pontos explícitos de extensão sem alterar o core. Hooks que recebem DataFrame devem retornar um DataFrame.

```python
from contractforge import IngestionHooks, ingest
from pyspark.sql import functions as F

hooks = IngestionHooks(
    after_prepare=lambda df, plan: df.withColumn("processed_by", F.lit(plan.notebook_name)),
)

ingest(
    source="raw.orders",
    target_table="b_orders",
    layer="bronze",
    hooks=hooks,
)
```

Falhas em hooks propagam como falha da ingestão e são registradas em `ctrl_ingestion_errors`.

### 16B.2 Registry de Write Modes

`register_write_mode(mode, handler)` adiciona motores de escrita customizados. O handler recebe `(plan, df, target, effective_rows)` e retorna o número lógico de linhas afetadas.

```python
from contractforge import register_write_mode

def my_writer(plan, df, target, effective_rows):
    df.write.format("delta").mode("append").saveAsTable(target)
    return effective_rows

register_write_mode("custom_append", my_writer)
```

### 16B.3 Registry de Sources

`register_source_resolver(source_type, resolver)` adiciona conectores declarativos sem alterar o core. O contrato aceita qualquer `source.connector` com nome válido (`letras`, `números`, `_` e `-`, começando por letra); na execução, o registry precisa ter um resolver registrado para esse nome.

Para batch, implemente `resolve_batch(spec, plan)` e retorne `SourceResolution`. Para streaming finito, implemente `resolve_stream(spec, plan)` e retorne `(stream_df, source_label)`.

Resolvers nativos registrados incluem `autoloader`, `table`, `delta_table`, `view`, `sql`, `parquet`, `json`, `csv`, `text`, `http_file`, `http_csv`, `object_storage`, `blob`, `jdbc` e `rest_api`.

Use a CLI para auditar capabilities disponíveis no runtime atual:

```bash
contractforge connectors list
contractforge connectors show rest_api
```

---

## 17. Padrões e Recomendações por Camada

### 17.1 Bronze

| Recomendação | Detalhe |
|-------------|---------|
| Modo | `scd0_append` (padrão) |
| Schema policy | `"permissive"` — origem pode ser instável |
| Watermark | Sempre que a origem tiver coluna confiável |
| Quality gates | Mínimo: `not_null` nas chaves, se possível |
| Encoding | `fix_encoding=True` se origem tem charset problemático |
| Partição | Por `ingestion_date` (partição técnica) |

### 17.2 Silver

| Recomendação | Detalhe |
|-------------|---------|
| Modo | `scd1_upsert` para estado atual; `scd2_historical` para histórico |
| Schema policy | `"additive_only"` — contrato que só cresce |
| Deduplicação | Sempre defina `dedup_order_expr` |
| Quality gates | `not_null` + `unique_key` nas chaves; `accepted_values` em enums |
| SCD2 | Restrinja `scd2_change_columns` ao mínimo de negócio |
| Snapshot | Só use se a origem for realmente completa |

### 17.3 Gold

| Recomendação | Detalhe |
|-------------|---------|
| Modo | `scd0_overwrite` para reconstruções; `scd1_upsert` para manutenção |
| Schema policy | `"strict"` — contrato fixo |
| Quality gates | `required_columns` + `min_rows` para garantir completude |
| Otimização | `optimize_after_write=True` com `zorder_columns` nas colunas de consulta |
| Cuidado | Evite `OPTIMIZE` automático em tabelas pequenas ou microcargas |

### 17.4 Convenções de Nomenclatura

| Camada | Prefixo | Exemplo |
|--------|---------|---------|
| Bronze | `b_` | `b_orders`, `b_events` |
| Silver | `c_` | `c_orders`, `c_cliente` |
| Gold — Dimensão | `dim_` | `dim_cliente`, `dim_produto` |
| Gold — Fato | `fato_` ou `f_` | `fato_vendas`, `f_pedidos_diario` |
| Gold — Agregado | `gd_` | `gd_metricas_diario` |

---

## 18. Exemplos Completos

### 18.1 Bronze — Append Incremental

**Python:**
```python
from contractforge import ingest

result = ingest(
    source="raw_erp_orders",
    target_table="b_erp_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    source_system="erp",
    watermark_columns="updated_at",
    schema_policy="permissive",
    notebook_name="bronze_erp_orders",
    description="Captura bruta de pedidos do ERP",
    owner="data-platform",
    domain="vendas",
)
```

**YAML (`contracts/bronze/b_erp_orders.yaml`):**
```yaml
source: raw_erp_orders
target_table: b_erp_orders
catalog: main
layer: bronze
mode: scd0_append
source_system: erp
watermark_columns: updated_at
schema_policy: permissive
notebook_name: bronze_erp_orders
description: "Captura bruta de pedidos do ERP"
owner: data-platform
domain: vendas
tags: [bronze, erp, pedidos]
```

### 18.2 Silver — SCD1 Upsert com Qualidade e Quarentena

**Python:**
```python
result = ingest(
    source="b_erp_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    source_system="erp",
    merge_keys="order_id",
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "not_null": ["order_id", "updated_at"],
        "accepted_values": {"status": ["open", "closed", "cancelled"]},
        "max_null_ratio": {"customer_email": 0.20},
    },
    on_quality_fail="quarantine",  # apenas regras de linha são quarentenadas
    explain_mode=True,
    openlineage_enabled=True,
    lock_enabled=True,
    description="Pedidos padronizados com deduplicação",
    owner="data-platform",
    domain="vendas",
    tags=["silver", "pedidos"],
)

if result["status"] != "SUCCESS":
    raise RuntimeError(f"Ingestão falhou: {result['error_message']}")

print(f"Escritas: {result['rows_written']}, Quarentena: {result['rows_quarantined']}")
print(f"Versão Delta: {result['delta_version_before']} → {result['delta_version_after']}")
```

**YAML (`contracts/silver/c_orders.yaml`):**
```yaml
source: b_erp_orders
target_table: c_orders
catalog: main
layer: silver
mode: scd1_upsert
source_system: erp
merge_keys: order_id
watermark_columns: updated_at
dedup_order_expr: "updated_at DESC NULLS LAST"
schema_policy: additive_only
quality_rules:
  not_null: [order_id, updated_at]
  accepted_values:
    status: [open, closed, cancelled]
  max_null_ratio:
    customer_email: 0.20
on_quality_fail: quarantine
explain_mode: true
openlineage_enabled: true
lock_enabled: true
notebook_name: silver_orders
description: "Pedidos padronizados com deduplicação"
owner: data-platform
domain: vendas
tags: [silver, pedidos, erp]
```

### 18.3 Silver — SCD2 Histórico

**Python:**
```python
result = ingest(
    source="c_orders",
    target_table="dim_order_status_history",
    catalog="main",
    layer="silver",
    mode="scd2_historical",
    merge_keys="order_id",
    scd2_change_columns="status|total_value|shipping_address",
    scd2_effective_from_column="updated_at",
    cluster_columns="order_id|status",
    schema_policy="additive_only",
    lock_enabled=True,
    openlineage_enabled=True,
    description="Histórico de alterações de status de pedidos",
    owner="data-platform",
    domain="vendas",
)
```

**YAML (`contracts/silver/dim_order_status_history.yaml`):**
```yaml
source: c_orders
target_table: dim_order_status_history
catalog: main
layer: silver
mode: scd2_historical
merge_keys: order_id
scd2_change_columns: status|total_value|shipping_address
scd2_effective_from_column: updated_at
cluster_columns: order_id|status
schema_policy: additive_only
lock_enabled: true
openlineage_enabled: true
notebook_name: silver_order_scd2
description: "Histórico de alterações de status de pedidos"
owner: data-platform
domain: vendas
```

### 18.4 Silver — Snapshot com Soft Delete

**Python:**
```python
result = ingest(
    source="snapshot_customers_today",
    target_table="c_customer_snapshot",
    catalog="main",
    layer="silver",
    mode="snapshot_soft_delete",
    merge_keys="customer_id",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "required_columns": ["customer_id"],
        "not_null": ["customer_id"],
    },
    on_quality_fail="fail",
    description="Snapshot diário de clientes ativos com soft delete",
    owner="data-platform",
    domain="comercial",
    runtime_parameters={"carga": "snapshot_completo"},
)
```

**YAML (`contracts/silver/c_customer_snapshot.yaml`):**
```yaml
source: snapshot_customers_today
target_table: c_customer_snapshot
catalog: main
layer: silver
mode: snapshot_soft_delete
merge_keys: customer_id
dedup_order_expr: "updated_at DESC NULLS LAST"
schema_policy: additive_only
quality_rules:
  required_columns: [customer_id]
  not_null: [customer_id]
on_quality_fail: fail
notebook_name: silver_customer_snapshot
description: "Snapshot diário de clientes ativos com soft delete"
owner: data-platform
domain: comercial
runtime_parameters:
  carga: snapshot_completo
# snapshot_soft_delete NÃO aceita watermark_columns nem filter_expression
```

### 18.5 Gold — Overwrite Particionado

**Python:**
```python
result = ingest(
    source=df_gold_month,
    target_table="gd_sales_monthly",
    catalog="main",
    layer="gold",
    mode="scd0_overwrite",
    partition_column="month_ref",
    partition_value="2026-05",
    schema_policy="strict",
    optimize_after_write=True,
    zorder_columns="month_ref|region",
    description="Agregado mensal de vendas por região",
    owner="analytics",
    domain="vendas",
)
```

**YAML (`contracts/gold/gd_sales_monthly.yaml`):**
```yaml
source: c_vendas
target_table: gd_sales_monthly
catalog: main
layer: gold
mode: scd0_overwrite
partition_column: month_ref
partition_value: "{{dt}}"           # placeholder resolvido em runtime
schema_policy: strict
optimize_after_write: true
zorder_columns: month_ref|region
notebook_name: gold_sales_monthly
description: "Agregado mensal de vendas por região"
owner: analytics
domain: vendas
```

### 18.6 Dry Run — Validação sem Escrita

**Python:**
```python
# Seguro rodar contra produção — não cria tabelas, não escreve dados
result = ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id",
    watermark_columns="updated_at",
    quality_rules={
        "required_columns": ["order_id", "updated_at"],
        "not_null": ["order_id"],
    },
    dry_run=True,
)

print(f"Status: {result['status']}")                   # DRY_RUN
print(f"Linhas efetivas: {result['rows_effective']}")   # rows_read - rows_quarantined
print(f"Partições afetadas: {result['affected_partitions']}")
print(f"Schema changes: {result['schema_changes']}")
print(f"Watermark candidate: {result['watermark_candidate']}")
```

**YAML (`contracts/silver/c_orders_dry.yaml`):**
```yaml
source: b_orders
target_table: c_orders
catalog: main
layer: silver
mode: scd1_upsert
merge_keys: order_id
watermark_columns: updated_at
quality_rules:
  required_columns: [order_id, updated_at]
  not_null: [order_id]
dry_run: true     # valida tudo, não escreve nada
```

### 18.7 Hash Diff com Exclusão de Colunas Voláteis

**Python:**
```python
result = ingest(
    source="b_produto",
    target_table="c_produto_versions",
    catalog="main",
    layer="silver",
    mode="scd1_hash_diff",
    hash_keys="id_produto",
    hash_exclude_columns="updated_at|extraction_ts|source_file",
    dedup_order_expr="updated_at DESC NULLS LAST",
    partition_column="ingestion_date",
    schema_policy="additive_only",
    description="Catálogo de produtos com versionamento por hash diff",
    owner="master-data",
    domain="produtos",
)
```

**YAML (`contracts/silver/c_produto_versions.yaml`):**
```yaml
source: b_produto
target_table: c_produto_versions
catalog: main
layer: silver
mode: scd1_hash_diff
hash_keys: id_produto
hash_exclude_columns: updated_at|extraction_ts|source_file
dedup_order_expr: "updated_at DESC NULLS LAST"
partition_column: ingestion_date
schema_policy: additive_only
notebook_name: silver_produto_hash
description: "Catálogo de produtos com versionamento por hash diff"
owner: master-data
domain: produtos
```

### 18.8 Usando `ingest_plan` com YAML

```python
import yaml
from contractforge import ingest_plan
from contractforge.plan import build_plan_from_kwargs

with open("contracts/silver/c_clientes.yaml") as f:
    cfg = yaml.safe_load(f)

plan = build_plan_from_kwargs(**cfg)
result = ingest_plan(plan)
```

### 18.9 Custom Keys para Chave Composta

**Python:**
```python
ingest(
    source="raw_items",
    target_table="c_item",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    select_columns="empresa|filial|pedido|item|updated_at|valor",
    custom_keys={"id_item": ["empresa", "filial", "pedido", "item"]},
    merge_keys="id_item",  # chave única derivada
    dedup_order_expr="updated_at DESC NULLS LAST",
)
```

**YAML (`contracts/silver/c_item.yaml`):**
```yaml
source: raw_items
target_table: c_item
catalog: main
layer: silver
mode: scd1_upsert
select_columns: empresa|filial|pedido|item|updated_at|valor
custom_keys:
  id_item: empresa|filial|pedido|item   # coluna derivada = concat_ws("|", ...)
merge_keys: id_item
dedup_order_expr: "updated_at DESC NULLS LAST"
notebook_name: silver_item
```

---

## 19. Orquestração com Databricks Workflows

### 19.1 Padrão YAML + Notebook Genérico

```
contracts/
├── bronze/
│   ├── b_clientes.yaml
│   ├── b_pedidos.yaml
│   └── b_itens.yaml
├── silver/
│   ├── c_clientes.yaml
│   ├── c_pedidos.yaml
│   └── c_itens.yaml
└── gold/
    └── f_pedidos_diario.yaml
```

**Notebook genérico (`run_ingestion`):**
```python
import yaml
from contractforge import ingest_plan
from contractforge.plan import build_plan_from_kwargs

dbutils.widgets.text("contract_path", "")
dbutils.widgets.text("master_run_id", "")

contract_path = dbutils.widgets.get("contract_path")
master_run_id = dbutils.widgets.get("master_run_id") or None

with open(contract_path, "r") as f:
    cfg = yaml.safe_load(f)

if master_run_id:
    cfg.setdefault("master_run_id", master_run_id)

plan = build_plan_from_kwargs(**cfg)
result = ingest_plan(plan)

if result["status"] != "SUCCESS":
    raise RuntimeError(f"Ingestão falhou: {result.get('error_message')}")

dbutils.notebook.exit(json.dumps(result, default=str))
```

### 19.2 Databricks Asset Bundle com `for_each_task`

```yaml
# databricks.yml
resources:
  jobs:
    pipeline_diaria:
      name: pipeline_diaria
      schedule:
        quartz_cron_expression: "0 0 5 * * ?"
        timezone_id: America/Sao_Paulo
      tasks:
        - task_key: bronze_layer
          for_each_task:
            inputs: |
              [{"contract": "contracts/bronze/b_clientes.yaml"},
               {"contract": "contracts/bronze/b_pedidos.yaml"}]
            concurrency: 4
            task:
              notebook_task:
                notebook_path: ../notebooks/run_ingestion
                base_parameters:
                  contract_path: "{{input.contract}}"
                  master_run_id: "{{job.run_id}}"

        - task_key: silver_layer
          depends_on: [{task_key: bronze_layer}]
          for_each_task:
            inputs: |
              [{"contract": "contracts/silver/c_clientes.yaml"}]
            concurrency: 4
            task:
              notebook_task:
                notebook_path: ../notebooks/run_ingestion
                base_parameters:
                  contract_path: "{{input.contract}}"
                  master_run_id: "{{job.run_id}}"
```

---

## 20. Troubleshooting

### Erros Comuns

| Sintoma | Causa Provável | Solução |
|---------|---------------|---------|
| `RuntimeError: Nenhuma SparkSession ativa` | Código fora de Databricks sem sessão criada | Crie `SparkSession.builder...getOrCreate()` antes de `import contractforge` |
| `ModuleNotFoundError: No module named 'delta'` | Falta delta-spark fora do Databricks | `pip install "contractforge[spark]"` (já incluso no Databricks Runtime) |
| `ConcurrentAppendException` / conflito de commit | Escritas concorrentes na mesma tabela | Ative `lock_enabled=True`, reduza concorrência, use `delta_by_partition` |
| `Schema policy strict violada` | Schema da fonte divergiu do target | Mude para `additive_only`/`permissive` ou corrija a fonte |
| `quality.accepted_values.X possui N valores` | Lista > 1000 valores | Use tabela de referência + `LEFT ANTI JOIN` antes da chamada |
| `Bronze deve ser orientada a captura` | Usou `scd1_upsert`/`scd2_historical` em bronze | Use `scd0_append`/`scd0_overwrite`/`scd1_hash_diff` ou mude a layer |
| `snapshot_soft_delete exige snapshot completo` | Combinou `snapshot_soft_delete` com `watermark_columns`/`filter_expression` | Remova o filtro/watermark ou use `scd1_upsert` |
| `Regras abortivas não são quarentenáveis` | `unique_key`/`min_rows`/`required_columns` falhou com `on_quality_fail="quarantine"` | Use `on_quality_fail="warn"` ou corrija os dados |
| Watermark não avança | Execução falhou ou dados sem watermark | Verifique `ctrl_ingestion_state` e logs; corrija a falha primeiro |
| `MERGE source has multiple matches` | Duplicidade nas `merge_keys` | Use `dedup_order_expr` + `unique_key` nos quality gates |
| SCD2 gera versões demais | `scd2_change_columns` muito amplo ou hash incluindo colunas voláteis | Restrinja às colunas de negócio que realmente definem mudança |
| Explain vazio ou incompleto | Limitação de captura em serverless | Consulte Spark UI e `DESCRIBE HISTORY` complementarmente |

### Diagnóstico Rápido

```sql
-- Últimos erros
SELECT run_id, target_table, error_type, error_message, stack_trace
FROM ops.ctrl_ingestion_errors
WHERE error_date >= current_date() - 1
ORDER BY error_ts_utc DESC;

-- Tabelas com falhas recentes
SELECT target_table, last_status, last_error_message, last_success_at_utc
FROM ops.ctrl_ingestion_state
WHERE last_status = 'FAILED'
ORDER BY last_updated_at_utc DESC;

-- Evolução de schema detectada
SELECT target_table, change_type, column_name, source_type, target_type, applied, change_ts_utc
FROM ops.ctrl_ingestion_schema_changes
ORDER BY change_ts_utc DESC;
```

---

## 21. FAQ

**P: Posso usar o framework com Structured Streaming?**
Para streaming contínuo, não. A versão atual suporta Autoloader em `available_now`, que é uma execução finita com checkpoint e `foreachBatch`. Para processamento contínuo, considere Delta Live Tables (DLT) ou Structured Streaming direto.

**P: O framework suporta CDC (Change Data Feed) como origem?**
Não nativamente. Você pode processar o CDF antes e passar um DataFrame para o `ingest()`, mas o framework não lê o feed automaticamente.

**P: Como customizo os nomes das ctrl tables?**
Os nomes vêm de `FrameworkConfig.ctrl_table_*`. Para alterar, faça monkey-patch do `CONFIG` ou, preferencialmente, use `ctrl_schema` no plan para isolar ambientes.

**P: Como removo dados antigos das ctrl tables?**
Use o comando de manutenção. Sem `--apply`, ele apenas mostra o plano:

```bash
contractforge maintenance ctrl-retention --catalog main --ctrl-schema ops --retention-days 90
contractforge maintenance ctrl-retention --catalog main --ctrl-schema ops --retention-days 90 --vacuum --apply
```

**P: Posso usar `select_columns` para renomear colunas?**
Não. `select_columns` apenas seleciona colunas existentes. Para renomear, transforme o DataFrame antes de passar para `ingest()`.

**P: O que acontece se `dedup_order_expr` referencia coluna que não existe?**
O framework valida colunas referenciadas no plan e ergue `ValueError` se faltarem.

**P: `on_quality_fail="quarantine"` isola todas as falhas?**
Não. Apenas regras de linha: `not_null`, `accepted_values`, `max_null_ratio`, e `expressions` com `severity="quarantine"`. Regras de conjunto (`unique_key`, `min_rows`, `required_columns`) escalam para `fail`.

**P: Qual a diferença entre `merge_keys` e `hash_keys`?**
- `merge_keys`: usadas na cláusula `ON` do MERGE (`scd1_upsert`, `scd2_historical`, `snapshot_soft_delete`)
- `hash_keys`: usadas para encontrar o "último estado" no target em `scd1_hash_diff`

**P: Posso usar `partition_column` + `cluster_columns` juntos?**
Não. O framework trata como mutuamente exclusivos na criação da tabela — cluster tem prioridade.

**P: O framework aplica `VACUUM` automaticamente?**
Não. Manutenção de arquivos Delta (VACUUM, OPTIMIZE agendado) é responsabilidade do operador.

**P: Como testar um contrato YAML sem rodar de fato?**
Adicione `dry_run: true` no YAML ou passe `dry_run=True`. O framework valida schema, quality gates e watermark sem escrever dados nem criar ctrl tables.

---

## 22. Checklist Pré-Produção

- [ ] Pacote instalado no cluster (verificar com `import contractforge; print(contractforge.__version__)`)
- [ ] Schema `ops` existe e cluster tem `CREATE TABLE` nele
- [ ] Permissões UC concedidas: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT`
- [ ] Cada contrato tem `notebook_name` único e descritivo
- [ ] Metadados de contrato preenchidos: `description`, `owner`, `domain`, `tags`
- [ ] `merge_keys` / `hash_keys` validados contra amostras reais (verificar duplicatas)
- [ ] `quality_rules` com ao menos `not_null` nas chaves
- [ ] `schema_policy` definida adequadamente por camada
- [ ] Para SCD2: `scd2_change_columns` restrito às colunas de negócio
- [ ] Para snapshot: source realmente completo (sem watermark/filter)
- [ ] `dry_run=True` executado ao menos uma vez e resultado inspecionado
- [ ] `dedup_order_expr` definido quando há risco de múltiplas versões por chave
- [ ] `optimize_after_write` com `zorder_columns` avaliado (custo vs. benefício)
- [ ] Estratégia de merge (`delta_by_partition`/`replace_partitions`) definida para tabelas grandes
- [ ] Job/workflow com retry configurado (>=1 retry, >=30s intervalo)
- [ ] Summary task ou alerta SQL para `status=FAILED`
- [ ] OpenLineage habilitado se há collector externo
- [ ] Testes passam (`pytest`)
- [ ] Plano de limpeza das ctrl tables definido (retenção de N dias)

---

## 23. Matriz de Compatibilidade

### 23.1 Modos de Escrita por Runtime

| Modo | Databricks Classic | Databricks Serverless | PySpark + Delta Local |
|------|:---:|:---:|:---:|
| `scd0_append` | ✅ | ✅ | ✅ |
| `scd0_overwrite` | ✅ | ✅ | ✅ |
| `scd1_upsert` | ✅ | ✅ (via SQL MERGE) | ✅ |
| `scd1_hash_diff` | ✅ | ✅ | ✅ |
| `scd2_historical` | ✅ | ✅ (via SQL MERGE) | ✅ |
| `snapshot_soft_delete` | ✅ | ✅ (via SQL MERGE) | ✅ |

### 23.2 Conectores por Runtime

| Conector | Databricks Classic | Databricks Serverless | PySpark Local | Dependência externa | Observação |
|----------|:---:|:---:|:---:|---------------------|------------|
| `table`, `delta_table`, `view`, `sql` | ✅ | ✅ | ✅ | Spark catalog | Depende de permissões no catálogo/schema/tabela. |
| `parquet`, `delta`, `json`, `jsonl`, `ndjson`, `csv`, `orc`, `text`, `avro`, `xml` | ✅ | ✅ | ✅ | Spark/Hadoop file readers | Path precisa estar acessível ao Spark; `jsonl/ndjson` usam reader `json`; `xml` depende do suporte do runtime. |
| `http_file`, `http_csv`, `http_json`, `http_text` | ✅ | ✅ | ✅ | Biblioteca padrão Python | Baixa HTTP(S) no driver; não depende de Spark filesystem para `https://`. |
| `object_storage`, `blob`, `s3`, `adls`, `azure_blob`, `gcs` | ✅ | ✅ via External Location/Volume ou rede liberada | Parcial | Credenciais cloud no runtime/Unity Catalog | Para Azure Blob, SAS direto é suportado em classic/job cluster/local quando `spark.conf.set` é permitido; em serverless, prefira External Location/Volume. |
| `jdbc`, `postgres`, `postgresql`, `sqlserver`, `mysql`, `oracle` | ✅ | ✅ se driver disponível | ✅ se driver disponível | Driver JDBC | Use particionamento e `fetchsize` para volume grande. |
| `rest_api` | ✅ | ✅ | ✅ | Biblioteca padrão Python | Indicado para APIs paginadas de volume controlado. |
| `snowflake` | ✅ se conector instalado | ✅ se suportado pelo runtime | ✅ se instalado | Spark Snowflake connector | Delegado a `spark.read.format("snowflake")`. |
| `bigquery` | ✅ se conector instalado | ✅ se suportado pelo runtime | ✅ se instalado | Spark BigQuery connector | Delegado a `spark.read.format("bigquery")`. |
| `autoloader` | ✅ | ✅ | ❌ | Databricks Auto Loader | Apenas `available_now`. |

Referência completa: `docs/compatibilidade_conectores.md`.

### 23.3 Requisitos de Software

| Componente | Mínimo | Recomendado |
|-----------|--------|-------------|
| Python | 3.10 | 3.11+ |
| PySpark | 3.4 | 3.5.x |
| delta-spark | 3.0 | 3.x |
| Databricks Runtime | 13.3 LTS | 14.3 LTS+ |
| Java (standalone) | 11 | 17 |

### 23.4 Status dos Testes Locais

A suite completa da lib foi validada localmente com Spark/Delta standalone:

```text
152 passed
```

Ambiente usado na validação: Python 3.11, PySpark 3.5.x, delta-spark 3.x e Java disponível.
Em hosts sem runtime Spark/Delta funcional, `SKIP_SPARK_TESTS=1` continua disponível para
executar apenas os testes puros.

### 23.5 Estrutura do Pacote

```
src/contractforge/
├── __init__.py        # API pública (ingest, ingest_plan, IngestionPlan, etc.)
├── _spark.py          # Resolução lazy de SparkSession + serverless detection
├── _sql.py            # Helpers SQL (quoting, literais, validação)
├── cli.py             # CLI contractforge validate/schema
├── config.py          # FrameworkConfig singleton + tipos (Layer, WriteMode, etc.)
├── contract_schema.py # JSON Schema do contrato declarativo
├── hooks.py           # IngestionHooks
├── plan.py            # IngestionPlan, QualityRules, QualityExpression, build_plan_from_kwargs
├── presets.py         # Presets declarativos e registry de presets customizados
├── shape.py           # Shape declarativo para JSON, structs e arrays
├── sources.py         # Source resolvers declarativos
├── schema.py          # Hash determinístico, dedup, custom keys, encoding, schema policy
├── watermark.py       # Watermark tipado (encode/decode/apply/compute)
├── quality.py         # Quality gates single-pass + quarentena
├── state.py           # Ctrl tables, log_run, upsert_state, locks, retry
├── writers.py         # 6 motores de escrita + dispatcher
├── lineage.py         # Explain capture + OpenLineage
└── ingestion.py       # Orquestrador principal
```

---

## 24. Licença e Contribuição

**Licença:** MIT

**Repositório:** https://github.com/marquesantero/contractforge

**Issues:** https://github.com/marquesantero/contractforge/issues

**Changelog:** https://github.com/marquesantero/contractforge/blob/main/CHANGELOG.md

### Versionamento

O projeto segue versionamento semântico:
- **PATCH** (x.y.z): correção de bug sem mudança de contrato
- **MINOR** (1.x.0): novo recurso compatível ou endurecimento planejado
- **MAJOR** (x.0.0): mudança incompatível

### Convenções de Código

- **Listas em parâmetros string:** use `|` como separador. Ex.: `merge_keys="id|tenant_id"`. Em Python, prefira listas nativas.
- **Nomes de parâmetros:** exatamente como documentado. Parâmetros desconhecidos em `ingest()` geram `ValueError` — isso é intencional para evitar typos silenciosos.
- **Charset:** arquivos YAML devem ser UTF-8.

---

**Fim da documentação.** Reporte problemas ou sugira melhorias via GitHub Issues.
