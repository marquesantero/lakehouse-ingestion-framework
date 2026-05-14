# Guia de Uso — ContractForge

Guia prático passo a passo para testar e operar o framework. Cobre dois modos de uso (**pacote** instalado e **script** colado no notebook), padrão YAML + notebook genérico, e duas opções de orquestração no Databricks Workflows (**`for_each_task`** e **master notebook**).

> Para a referência técnica completa de cada submódulo, fluxo interno e decisões de design, ver [arquitetura.md](./arquitetura.md). Para a documentação oficial completa de uso e contratos, ver [oficial.md](./oficial.md).

---

## Sumário

1. [Quando usar pacote vs script](#1-quando-usar-pacote-vs-script)
2. [Modo Pacote](#2-modo-pacote)
   - [2.1 Pré-requisitos](#21-pré-requisitos)
   - [2.2 Instalação local para desenvolvimento](#22-instalação-local-para-desenvolvimento)
   - [2.3 Instalação no Databricks (cluster library)](#23-instalação-no-databricks-cluster-library)
   - [2.4 Instalação notebook-scoped (`%pip`)](#24-instalação-notebook-scoped-pip)
   - [2.5 Primeiro teste — `Hello, ingest`](#25-primeiro-teste--hello-ingest)
3. [Modo Script](#3-modo-script)
   - [3.1 Quando usar](#31-quando-usar)
   - [3.2 Notebook único com código embutido](#32-notebook-único-com-código-embutido)
   - [3.3 Limitações vs Pacote](#33-limitações-vs-pacote)
4. [Padrão YAML + notebook genérico](#4-padrão-yaml--notebook-genérico)
   - [4.1 Estrutura de pastas](#41-estrutura-de-pastas)
   - [4.2 Anatomia de um contrato YAML](#42-anatomia-de-um-contrato-yaml)
   - [4.3 Notebook genérico `run_ingestion`](#43-notebook-genérico-run_ingestion)
   - [4.4 Validação local do YAML](#44-validação-local-do-yaml)
5. [Orquestração — Opção A: `for_each_task` (recomendado)](#5-orquestração--opção-a-for_each_task-recomendado)
   - [5.1 Conceito](#51-conceito)
   - [5.2 Databricks Asset Bundle completo](#52-databricks-asset-bundle-completo)
   - [5.3 Lista dinâmica via discovery task](#53-lista-dinâmica-via-discovery-task)
   - [5.4 Repair, retry e summary nativo](#54-repair-retry-e-summary-nativo)
6. [Orquestração — Opção B: Master notebook (padrão clássico)](#6-orquestração--opção-b-master-notebook-padrão-clássico)
   - [6.1 Quando preferir](#61-quando-preferir)
   - [6.2 Notebook master por layer](#62-notebook-master-por-layer)
   - [6.3 Job com 3 tasks sequenciais](#63-job-com-3-tasks-sequenciais)
   - [6.4 Repair manual via `ctrl_ingestion_runs`](#64-repair-manual-via-ctrl_ingestion_runs)
7. [Summary e auditoria](#7-summary-e-auditoria)
8. [Testando localmente sem Databricks](#8-testando-localmente-sem-databricks)
9. [Troubleshooting](#9-troubleshooting)
10. [Checklist pré-produção](#10-checklist-pré-produção)
11. [FAQ](#11-faq)

---

## 1. Quando usar pacote vs script

| Cenário | Use Pacote | Use Script |
|---|---|---|
| Time com Databricks workflows estáveis | ✅ | |
| Migração rápida de código existente | | ✅ |
| Múltiplos clusters/jobs compartilhando | ✅ | |
| Sandbox / one-shot exploratório | | ✅ |
| Quer testes em CI | ✅ | |
| Acesso restrito a UC Volumes | | ✅ |
| Versionamento de releases | ✅ | |

**Regra prática:** se você pretende rodar este framework em produção em mais de um job ou cluster, **use o pacote**. O modo script é uma ponte temporária.

---

## 2. Modo Pacote

### 2.1 Pré-requisitos

- Python 3.10 ou superior.
- PySpark 3.4+ e delta-spark 3.0+ quando fora do Databricks. Em Databricks, ambos já vêm com o runtime e o wheel não tenta resolvê-los como dependências obrigatórias.
- Java 11+ (apenas se for rodar Spark fora do Databricks).
- Acesso de gravação a um catálogo Unity Catalog ou a um schema Hive.

### 2.2 Instalação local para desenvolvimento

```bash
# clonar o repo
git clone https://github.com/marquesantero/contractforge.git
cd contractforge

# ambiente isolado
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1

# instalação editável + extras de dev
pip install -e ".[dev]"

# rodar testes puros (rápido, sem Spark)
pytest tests/test_plan.py -v

# rodar suite completa (precisa de Java instalado)
pytest -v
```

Saída esperada do `pytest tests/test_plan.py`:

```
tests/test_plan.py::test_validate_write_mode_accepts_valid PASSED
tests/test_plan.py::test_build_plan_basic PASSED
tests/test_plan.py::test_build_plan_normalizes_pipe_separated_lists PASSED
...
========== 11 passed in 0.20s ==========
```

### 2.3 Instalação no Databricks (cluster library)

Recomendado para uso compartilhado em produção.

**Passo 1 — Build do wheel:**

```bash
pip install build
python -m build
# gera: dist/contractforge-1.9.0-py3-none-any.whl
```

**Passo 2 — Upload para Unity Catalog Volume:**

```bash
# via Databricks CLI
databricks fs cp dist/contractforge-1.9.0-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/libs/
```

Ou pela UI: **Catalog → Volumes → Upload to volume**.

**Passo 3 — Instalar no cluster:**

1. Compute → seu cluster → Libraries → **Install new**
2. Source: **Volume**
3. File path: `/Volumes/<catalog>/<schema>/libs/contractforge-1.9.0-py3-none-any.whl`
4. Install
5. Reinicie o cluster (a library só fica ativa após restart)

**Passo 4 — Verificar:**

Em qualquer notebook anexado ao cluster:

```python
import lakehouse_ingestion
print(lakehouse_ingestion.__version__)  # 1.9.0
from lakehouse_ingestion import ingest, IngestionPlan, QualityRules
```

### 2.4 Instalação notebook-scoped (`%pip`)

Funciona em **serverless** (que não aceita cluster libraries tradicionais) e em desenvolvimento iterativo.

```python
%pip install /Volumes/<catalog>/<schema>/libs/contractforge-1.9.0-py3-none-any.whl
```

Se o cluster não permite `%pip` por restrição:

```python
%pip install --index-url https://<seu_pypi_privado> contractforge==1.9.0
```

Em seguida:

```python
dbutils.library.restartPython()
from lakehouse_ingestion import ingest
```

> `%pip install` instala **só na sessão do notebook**. Outros notebooks no mesmo cluster não enxergam.

### 2.5 Primeiro teste — `Hello, ingest`

Crie um DataFrame de teste e ingerir em uma tabela bronze:

```python
from lakehouse_ingestion import ingest
from pyspark.sql import functions as F

# DataFrame in-memory
df = spark.createDataFrame(
    [
        (1, "Alice", "2024-01-01"),
        (2, "Bob",   "2024-01-02"),
        (3, "Carol", "2024-01-03"),
    ],
    "id_cliente long, nome string, updated_at string",
).withColumn("updated_at", F.to_timestamp("updated_at"))

result = ingest(
    source=df,
    target_table="b_clientes_test",
    catalog="sandbox",                # use um catálogo onde você tem permissão
    layer="bronze",
    mode="scd0_append",
    notebook_name="hello_ingest",
    explain_mode=True,                # captura plano Spark em ctrl_ingestion_explain
)

print("status:        ", result["status"])
print("rows_written:  ", result["rows_written"])
print("delta_version: ", result["delta_version_after"])
print("run_id:        ", result["run_id"])
```

Saída esperada:

```
status:         SUCCESS
rows_written:   3
delta_version:  0
run_id:         a1b2c3d4-...
```

Verifique a tabela e os logs de controle:

```sql
-- a tabela criada
SELECT * FROM sandbox.bronze.b_clientes_test;

-- run registrada
SELECT run_id, status, rows_written, duration_seconds
FROM sandbox.ops.ctrl_ingestion_runs
WHERE target_table = 'sandbox.bronze.b_clientes_test'
ORDER BY started_at_utc DESC LIMIT 1;

-- estado mais recente
SELECT * FROM sandbox.ops.ctrl_ingestion_state
WHERE target_table = 'sandbox.bronze.b_clientes_test';

-- plano capturado
SELECT plan_text FROM sandbox.ops.ctrl_ingestion_explain
WHERE run_id = '<seu_run_id>';
```

Se chegou até aqui, a instalação está saudável. Próximo passo: configurar contratos YAML.

---

## 3. Modo Script

### 3.1 Quando usar

- Você ainda não tem permissão para subir wheel num UC Volume.
- Quer testar o framework antes de empacotar.
- Time pequeno fazendo prova de conceito.
- Restrição de governança (cluster sem permissão de `%pip`).

### 3.2 Notebook único com código embutido

**Opção 1 — Código embutido no notebook**

Não recomendado para o pacote atual, que tem múltiplos módulos e contrato de distribuição por wheel. Use apenas como recurso temporário se você mantiver uma cópia monolítica interna.

**Opção 2 — `%run` apontando para um notebook helper**

Crie um notebook `_lakehouse_ingestion_inline` no Workspace contendo a versão monolítica do código (todas as funções em um arquivo só). Depois, em qualquer notebook de ingestão:

```python
%run /Workspace/.../helpers/_lakehouse_ingestion_inline
```

E use:

```python
result = ingest(
    source=df,
    target_table="b_clientes_test",
    catalog="sandbox",
    layer="bronze",
    mode="scd0_append",
    notebook_name="ingestion_clientes",
)
```

Saída e ctrl tables são idênticos ao modo pacote.

**Opção 3 — Workspace Files (Files in Repos)**

Salve `ingestion.py` em `/Workspace/Repos/<você>/utils/`, garanta que o repo está sincronizado, e:

```python
import sys
sys.path.append("/Workspace/Repos/<você>/utils")
from ingestion import ingest, IngestionPlan
```

### 3.3 Limitações vs Pacote

| Aspecto | Modo Script | Modo Pacote |
|---|---|---|
| Versionamento | manual | semver via wheel |
| Dependências | manual | `pyproject.toml` |
| Submódulos | precisa juntar | importáveis (`from lakehouse_ingestion.quality import ...`) |
| Testes em CI | difícil | `pytest` direto |
| Atualização | re-cole o código | `pip install --upgrade` |
| Compartilhar entre clusters | copiar | uma vez como library |

---

## 4. Padrão YAML + notebook genérico

Em vez de um notebook por tabela, mantenha **um único notebook genérico** que recebe o caminho de um YAML descrevendo o contrato. Cada tabela vira um YAML de ~20 linhas.

### 4.1 Estrutura de pastas

Sugestão (em UC Volume ou Repo):

```
/Volumes/sandbox/lakehouse/
└── contracts/
    ├── bronze/
    │   ├── b_clientes.yaml
    │   ├── b_pedidos.yaml
    │   └── b_itens_pedido.yaml
    ├── silver/
    │   ├── c_clientes.yaml
    │   ├── c_pedidos.yaml
    │   └── c_itens_pedido.yaml
    └── gold/
        └── f_pedidos_diario.yaml
```

Cada YAML é um contrato declarativo da ingestão dessa tabela.

### 4.2 Anatomia de um contrato YAML

**`contracts/silver/c_clientes.yaml`**

```yaml
# obrigatórios
target_table: c_clientes
catalog: sandbox
layer: silver
mode: scd1_upsert
source: b_clientes              # str: nome de tabela; será resolvido com spark.read.table

# identificação operacional
source_system: crm
notebook_name: ingest_silver_clientes
ctrl_schema: ops
description: "Clientes consolidados do CRM"
owner: dados-clientes
domain: comercial
tags: [silver, cliente, crm]
sla: "D+0 08:00"
runtime_parameters:
  carga: incremental

# chaves e watermark
merge_keys: id_cliente
watermark_columns: updated_at
dedup_order_expr: "updated_at DESC NULLS LAST"

# políticas
schema_policy: additive_only
allow_type_widening: false

# qualidade
quality_rules:
  required_columns: [id_cliente, updated_at]
  not_null: [id_cliente]
  unique_key: [id_cliente]
  min_rows: 1
on_quality_fail: fail            # fail | warn | quarantine

# observabilidade
explain_mode: false
openlineage_enabled: true
openlineage_namespace: databricks://sandbox

# performance
use_cache: true
optimize_after_write: false
zorder_columns: []

# concorrência
lock_enabled: false
```

**`contracts/silver/c_pedidos.yaml`** — exemplo SCD2 com mudança rastreada:

```yaml
target_table: c_pedidos
catalog: sandbox
layer: silver
mode: scd2_historical
source: b_pedidos
source_system: erp
notebook_name: ingest_silver_pedidos

merge_keys: id_pedido
scd2_change_columns: [status, valor_total]
scd2_effective_from_column: data_atualizacao

watermark_columns: data_atualizacao
schema_policy: additive_only

quality_rules:
  not_null: [id_pedido, data_atualizacao]
  accepted_values:
    status: [PENDENTE, PAGO, CANCELADO, ENTREGUE]
  expressions:
    - name: valor_total_positivo
      expression: "valor_total > 0"
      severity: quarantine
      message: "Valor total deve ser positivo."
# `quarantine` só isola linhas atingidas por not_null/accepted_values/max_null_ratio.
# expressions com severity=quarantine também são isoláveis.
# Regras de conjunto (unique_key, min_rows, required_columns) escalam para fail.
on_quality_fail: quarantine
```

**`contracts/gold/f_pedidos_diario.yaml`** — fato agregado, overwrite por partição:

```yaml
target_table: f_pedidos_diario
catalog: sandbox
layer: gold
mode: scd0_overwrite
source: c_pedidos
source_system: erp
notebook_name: build_gold_pedidos_diario

partition_column: dt_referencia
partition_value: "{{dt}}"        # vamos resolver no notebook genérico

schema_policy: strict
quality_rules:
  required_columns: [dt_referencia, total_pedidos, valor_total]
  min_rows: 1

zorder_columns: [dt_referencia]
optimize_after_write: true
```

### 4.3 Notebook genérico `run_ingestion`

Salve como `/Workspace/.../run_ingestion`:

```python
# Databricks notebook source
import yaml
from pathlib import Path
from lakehouse_ingestion import ingest_plan, validate_plan_shape
from lakehouse_ingestion.plan import build_plan_from_kwargs

# Widgets — recebidos do orchestrator (for_each_task ou master)
dbutils.widgets.text("contract_path", "")
dbutils.widgets.text("master_run_id", "")
dbutils.widgets.text("dt", "")            # opcional, para overrides

contract_path = dbutils.widgets.get("contract_path")
master_run_id = dbutils.widgets.get("master_run_id") or None
dt            = dbutils.widgets.get("dt") or None

# 1. Lê o YAML
with open(contract_path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

# 2. Override de runtime (placeholders {{...}} no YAML)
def _render(value):
    if isinstance(value, str) and "{{" in value:
        return value.replace("{{dt}}", dt or "")
    return value

cfg = {k: _render(v) for k, v in cfg.items()}

# 3. Linhagem operacional — propaga master_run_id
if master_run_id:
    cfg.setdefault("master_run_id", master_run_id)

# 4. Constrói e valida o plan sem tocar dados
plan = build_plan_from_kwargs(**cfg)
validate_plan_shape(plan)

# 5. Executa
result = ingest_plan(plan)

# 6. Logs e retorno para o orquestrador
print(f"status={result['status']} target={result['target_table']} "
      f"rows_written={result['rows_written']} "
      f"rows_quarantined={result['rows_quarantined']}")

if result["status"] != "SUCCESS":
    raise RuntimeError(f"Ingestão falhou: {result.get('error_message', 'desconhecido')}")

# Retorno para dbutils.notebook.run (master pode parsear)
import json
dbutils.notebook.exit(json.dumps(result, default=str))
```

**Características:**

- `build_plan_from_kwargs` valida campos desconhecidos, normaliza listas com `|` e rejeita `quality_rules` malformadas.
- `column_mapping` permite contratos com nomes source/target diferentes; colisões e colunas técnicas reservadas são bloqueadas.
- `delta_properties` aplica TBLPROPERTIES na criação da tabela Delta.
- `retry_attempts` e `retry_backoff_seconds` podem ser definidos por YAML quando um plano precisar de política própria.
- `validate_plan_shape` é validação pura de contrato; pode rodar em CI sem Spark.
- Placeholders `{{dt}}` permitem override de runtime sem editar o YAML.
- `idempotency_key` pode ser preenchido com o identificador do lote/job. Prefira `idempotency_policy: skip_if_success|fail_if_success|rerun_if_failed|always_run`.
- `master_run_id` é propagado para `ctrl_ingestion_runs`, viabilizando summary cross-execução.
- `raise` em falha garante que a task aparece como failed no Workflow.
- `dbutils.notebook.exit(json.dumps(result))` permite o master, se houver, capturar o resultado.

### 4.4 Validação local do YAML

Antes de subir um YAML, valide localmente que ele é parseável e produz um `IngestionPlan` válido:

```bash
contractforge validate contracts/silver/c_pedidos.yaml
contractforge schema > lakehouse_ingestion.schema.json
```

```python
# tests/test_contracts.py
import pytest, yaml
from pathlib import Path
from lakehouse_ingestion.plan import build_plan_from_kwargs

CONTRACTS = Path("contracts").rglob("*.yaml")

@pytest.mark.parametrize("path", list(CONTRACTS))
def test_contract_is_valid(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # remove placeholders para o teste
    for k, v in cfg.items():
        if isinstance(v, str) and "{{" in v:
            cfg[k] = "PLACEHOLDER"
    plan = build_plan_from_kwargs(**cfg)
    assert plan.target_table
    assert plan.mode in {
        "scd0_append", "scd0_overwrite", "scd1_upsert",
        "scd1_hash_diff", "scd2_historical", "snapshot_soft_delete",
    }
```

Rodar:

```bash
pytest tests/test_contracts.py -v
```

Cada novo YAML que entrar no repo é automaticamente validado pelo CI.

### 4.5 Fontes e conectores declarativos

Para landing zones em arquivo, use Auto Loader `available_now` no formato unificado de conector:

```yaml
source:
  type: connector
  connector: autoloader
  path: /Volumes/main/raw/orders
  format: parquet
  read:
    schema_location: /Volumes/main/ops/schemas/orders
    checkpoint_location: /Volumes/main/ops/checkpoints/orders
    include_existing_files: true

target_table: b_orders
catalog: main
layer: bronze
mode: scd0_append
source_system: landing
ctrl_schema: ops
notebook_name: ingest_b_orders
```

O stream é finito: processa os arquivos disponíveis e encerra. A execução externa aparece em `ctrl_ingestion_streams`, e cada batch aparece em `ctrl_ingestion_runs`.

Para arquivos batch, JDBC e REST API, use o mesmo campo `source.type=connector`:

```yaml
source:
  type: connector
  connector: object_storage
  provider: s3
  format: parquet
  path: s3://empresa-landing/orders/
  read:
    source_complete: true

target_table: snapshot_orders
catalog: main
layer: silver
mode: snapshot_soft_delete
merge_keys: [order_id]
```

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
  auth:
    type: oauth_client_credentials
    token_url: https://login.example.com/oauth/token
    client_id: "{{ secret:orders_api/client_id }}"
    client_secret: "{{ secret:orders_api/client_secret }}"
    scope: orders.read
  pagination:
    type: link_header
  response:
    records_path: $.data
  incremental:
    watermark_param: updated_after
    initial_value: "1970-01-01T00:00:00Z"
  limits:
    max_pages: 25
    timeout_seconds: 60
    retry_attempts: 3
    rate_limit_per_minute: 120

target_table: b_orders_api
catalog: main
layer: bronze
mode: scd0_append
```

Secrets no formato `{{ secret:scope/key }}` são resolvidos via Databricks Secrets ou variável de ambiente `CONTRACTFORGE_SECRET_SCOPE_KEY`. As ctrl tables recebem metadados redigidos do source.

A coluna `ctrl_ingestion_runs.source_metrics_json` complementa esses metadados com métricas operacionais do conector. Em REST, ela registra `request_count`, `pages_read`, `records_read`, `bytes_read`, tipo de paginação, retry/rate limit e watermark aplicado. Em JDBC, registra estratégia de leitura, incrementalidade aplicada, watermark, particionamento e `fetchsize`. Em tabelas, SQL e arquivos, registra a estratégia Spark usada e se a fonte foi declarada como completa.

Descubra os conectores disponíveis sem Spark:

```bash
contractforge connectors list
contractforge connectors show rest_api jdbc autoloader
contractforge connectors doctor rest_api jdbc autoloader
```

`contractforge validate` também valida os campos obrigatórios dos conectores nativos, evitando descobrir em runtime que faltou `source.request.url`, `source.options.url`, `source.read.checkpoint_location` ou configuração completa de particionamento JDBC.

Para cargas incrementais, combine o watermark normal da lib com `source.incremental`. O framework busca o watermark salvo antes de resolver a fonte e injeta esse valor no conector:

```yaml
watermark_columns: updated_at

source:
  type: connector
  connector: jdbc
  options:
    url: "{{ secret:erp/jdbc_url }}"
    dbtable: public.orders
  incremental:
    watermark_column: updated_at
    initial_value: "1970-01-01 00:00:00"
```

---

## 5. Orquestração — Opção A: `for_each_task` (recomendado)

### 5.1 Conceito

`for_each_task` é uma feature nativa do Databricks Workflows: você define **uma task** com uma **lista de inputs**, e o Databricks executa **uma sub-task por input**, com **identidade própria** para cada uma.

Vantagens:

- **Repair granular** — reexecuta apenas as iterações que falharam.
- **Concorrência configurável** — limite de paralelismo nativo.
- **UI nativa de status** — cada iteração com duração, retry e logs separados.
- **Adicionar/remover tabelas** = só editar a lista (ou apontá-la para uma tabela/arquivo dinâmico).
- **Retry independente** por iteração.

Disponível em DBR ≥ 13.3 LTS.

### 5.2 Databricks Asset Bundle completo

Crie `databricks.yml` na raiz do projeto:

```yaml
bundle:
  name: contractforge

variables:
  catalog:
    description: "Catálogo Unity onde rodar"
    default: sandbox
  contracts_root:
    description: "Raiz dos contratos YAML"
    default: /Volumes/sandbox/lakehouse/contracts

resources:
  jobs:
    pipeline_diaria:
      name: pipeline_diaria
      schedule:
        quartz_cron_expression: "0 0 5 * * ?"   # 05:00 UTC
        timezone_id: America/Sao_Paulo

      job_clusters:
        - job_cluster_key: shared
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_D4ds_v5
            num_workers: 2

      tasks:
        # ----- BRONZE -----
        - task_key: bronze_layer
          for_each_task:
            inputs: |
              [
                {"contract": "${var.contracts_root}/bronze/b_clientes.yaml"},
                {"contract": "${var.contracts_root}/bronze/b_pedidos.yaml"},
                {"contract": "${var.contracts_root}/bronze/b_itens_pedido.yaml"}
              ]
            concurrency: 4
            task:
              job_cluster_key: shared
              notebook_task:
                notebook_path: ../notebooks/run_ingestion
                base_parameters:
                  contract_path: "{{input.contract}}"
                  master_run_id: "{{job.run_id}}"

        # ----- SILVER -----
        - task_key: silver_layer
          depends_on:
            - task_key: bronze_layer
          for_each_task:
            inputs: |
              [
                {"contract": "${var.contracts_root}/silver/c_clientes.yaml"},
                {"contract": "${var.contracts_root}/silver/c_pedidos.yaml"},
                {"contract": "${var.contracts_root}/silver/c_itens_pedido.yaml"}
              ]
            concurrency: 4
            task:
              job_cluster_key: shared
              notebook_task:
                notebook_path: ../notebooks/run_ingestion
                base_parameters:
                  contract_path: "{{input.contract}}"
                  master_run_id: "{{job.run_id}}"

        # ----- GOLD -----
        - task_key: gold_layer
          depends_on:
            - task_key: silver_layer
          for_each_task:
            inputs: |
              [
                {"contract": "${var.contracts_root}/gold/f_pedidos_diario.yaml"}
              ]
            concurrency: 2
            task:
              job_cluster_key: shared
              notebook_task:
                notebook_path: ../notebooks/run_ingestion
                base_parameters:
                  contract_path: "{{input.contract}}"
                  master_run_id: "{{job.run_id}}"
                  dt: "{{job.start_time | date('yyyy-MM-dd')}}"

        # ----- SUMMARY -----
        - task_key: summary
          depends_on:
            - task_key: gold_layer
          run_if: ALL_DONE                    # roda mesmo se algum failed
          notebook_task:
            notebook_path: ../notebooks/summary
            base_parameters:
              master_run_id: "{{job.run_id}}"
```

**Deploy:**

```bash
# CLI
databricks bundle validate
databricks bundle deploy --target prod
databricks bundle run pipeline_diaria
```

### 5.3 Lista dinâmica via discovery task

Em vez de hardcoded, descubra os YAMLs em runtime:

```yaml
tasks:
  - task_key: discover_silver
    notebook_task:
      notebook_path: ../notebooks/discover_contracts
      base_parameters:
        layer: silver
        contracts_root: ${var.contracts_root}

  - task_key: silver_layer
    depends_on:
      - task_key: discover_silver
    for_each_task:
      inputs: "{{tasks.discover_silver.values.contracts}}"
      concurrency: 4
      task:
        notebook_task:
          notebook_path: ../notebooks/run_ingestion
          base_parameters:
            contract_path: "{{input.contract}}"
            master_run_id: "{{job.run_id}}"
```

**`notebooks/discover_contracts`:**

```python
import json
from pathlib import Path

dbutils.widgets.text("layer", "silver")
dbutils.widgets.text("contracts_root", "")

layer = dbutils.widgets.get("layer")
root  = dbutils.widgets.get("contracts_root")

# lista todos os YAMLs da camada
contracts = sorted(str(p) for p in Path(f"{root}/{layer}").glob("*.yaml"))

# inputs no formato esperado pelo for_each_task
payload = [{"contract": c} for c in contracts]

# expõe para a próxima task via taskValues
dbutils.jobs.taskValues.set(key="contracts", value=payload)
print(f"Descobertos {len(payload)} contratos em {layer}")
```

A partir daqui, **basta soltar um novo YAML** na pasta para que ele entre no pipeline na próxima execução.

### 5.4 Repair, retry e summary nativo

**Repair** (após falha):
1. UI do job → run que falhou → **Repair**.
2. Selecione as tasks/iterações que devem rodar de novo.
3. Confirme.

O Databricks reexecuta **somente** o que foi marcado, mantendo o `run_id` original. Suas ctrl tables registram cada repair como um novo `run_id` interno do framework, mas correlacionados via `master_run_id`.

**Retry automático** (configurável por task):

```yaml
- task_key: silver_layer
  max_retries: 2
  min_retry_interval_millis: 30000
  for_each_task: ...
```

**Summary nativo:** a UI já mostra cada iteração com duração, status e logs. O `summary` task adicional é útil para envio externo (Slack, e-mail).

---

## 6. Orquestração — Opção B: Master notebook (padrão clássico)

### 6.1 Quando preferir

- Você precisa de **lógica condicional pesada** entre tabelas (ex.: "só roda silver_pedidos se silver_clientes teve >0 linhas").
- Está em **DBR antigo** sem `for_each_task`.
- Migração de um pipeline que já existe nesse padrão e mexer dá retrabalho.
- **Trade-off**: você perde o repair granular nativo. Reparar exige re-rodar o master inteiro (ou implementar repair manual — ver §6.4).

### 6.2 Notebook master por layer

**`notebooks/master_silver`:**

```python
# Databricks notebook source
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

dbutils.widgets.text("contracts_root", "/Volumes/sandbox/lakehouse/contracts")
dbutils.widgets.text("max_concurrency", "4")
dbutils.widgets.text("master_run_id", "")
dbutils.widgets.text("layer", "silver")

contracts_root = dbutils.widgets.get("contracts_root")
max_conc       = int(dbutils.widgets.get("max_concurrency"))
master_run_id  = dbutils.widgets.get("master_run_id") or str(uuid.uuid4())
layer          = dbutils.widgets.get("layer")

# 1. Descobre contratos da camada
contracts = sorted(str(p) for p in Path(f"{contracts_root}/{layer}").glob("*.yaml"))
print(f"[master_{layer}] master_run_id={master_run_id} | {len(contracts)} contratos")

# 2. Executa em paralelo
def run_one(contract_path: str) -> dict:
    raw = dbutils.notebook.run(
        "/Workspace/.../run_ingestion",
        timeout_seconds=3600,
        arguments={
            "contract_path": contract_path,
            "master_run_id": master_run_id,
        },
    )
    return {"contract": contract_path, "result": json.loads(raw)}

results = []
with ThreadPoolExecutor(max_workers=max_conc) as ex:
    futures = {ex.submit(run_one, c): c for c in contracts}
    for fut in as_completed(futures):
        contract = futures[fut]
        try:
            r = fut.result()
            print(f"  ✓ {Path(contract).name}: status={r['result']['status']} "
                  f"rows={r['result']['rows_written']}")
            results.append(r)
        except Exception as e:
            print(f"  ✗ {Path(contract).name}: {e}")
            results.append({"contract": contract, "error": str(e)})

# 3. Summary local (complementa o que fica em ctrl_ingestion_runs)
ok    = [r for r in results if r.get("result", {}).get("status") == "SUCCESS"]
fail  = [r for r in results if r.get("result", {}).get("status") == "FAILED"
                              or "error" in r]

print()
print(f"[master_{layer}] OK={len(ok)} FAIL={len(fail)} TOTAL={len(results)}")
print(f"  rows_written total: {sum(r['result']['rows_written'] for r in ok):,}")
print(f"  rows_quarantined:   {sum(r['result']['rows_quarantined'] for r in ok):,}")

if fail:
    print(f"\n[master_{layer}] FALHAS:")
    for r in fail:
        name = Path(r['contract']).name
        msg  = r.get('result', {}).get('error_message') or r.get('error')
        print(f"  - {name}: {str(msg)[:300]}")
    raise RuntimeError(f"{len(fail)} ingestões falharam em {layer}")

# 4. Retorno (para o job ou para um master_global)
dbutils.notebook.exit(json.dumps({
    "layer": layer,
    "master_run_id": master_run_id,
    "ok": len(ok),
    "fail": len(fail),
    "rows_written": sum(r["result"]["rows_written"] for r in ok),
}))
```

### 6.3 Job com 3 tasks sequenciais

```yaml
resources:
  jobs:
    pipeline_diaria_master:
      name: pipeline_diaria_master
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: ../notebooks/master_bronze
            base_parameters:
              layer: bronze
              master_run_id: "{{job.run_id}}"

        - task_key: silver
          depends_on: [{task_key: bronze}]
          notebook_task:
            notebook_path: ../notebooks/master_silver
            base_parameters:
              layer: silver
              master_run_id: "{{job.run_id}}"

        - task_key: gold
          depends_on: [{task_key: silver}]
          notebook_task:
            notebook_path: ../notebooks/master_gold
            base_parameters:
              layer: gold
              master_run_id: "{{job.run_id}}"
```

**Repair nativo:** se `silver` falhar, você pode reparar **só `silver` e `gold`**. Mas dentro do master_silver, todas as 30 tabelas rodarão de novo (não há granularidade por tabela).

### 6.4 Repair manual via `ctrl_ingestion_runs`

Para "simular repair" e pular tabelas que já tiveram SUCCESS no mesmo `master_run_id`, ajuste o master:

```python
# ANTES do loop, descobre o que já passou nesta master_run
already_ok = {
    r.target_table.split(".")[-1]                # nome puro da tabela
    for r in spark.sql(f"""
        SELECT DISTINCT target_table
        FROM ops.ctrl_ingestion_runs
        WHERE master_run_id = '{master_run_id}'
          AND status = 'SUCCESS'
    """).collect()
}

def already_done(contract_path: str) -> bool:
    cfg = yaml.safe_load(open(contract_path))
    return cfg["target_table"] in already_ok

todo = [c for c in contracts if not already_done(c)]
print(f"[master_{layer}] {len(already_ok)} já OK, {len(todo)} a executar")

# resto do loop, agora sobre `todo`
```

Re-execute o master passando o **mesmo `master_run_id`** (via re-run com mesmos params no Databricks) e ele pula o que passou. **Limitação:** o Databricks não rastreia automaticamente — você precisa lembrar de manter o id; em Workflows, `{{job.run_id}}` muda em cada execução, então **repair nativo do Databricks não preserva o run_id**. Para esse modelo funcionar, você precisaria de uma "fila persistida" externa (ex.: outra ctrl table), o que começa a competir com `for_each_task`.

> Conclusão: se repair granular importa, vá de `for_each_task`. O master é melhor quando você precisa de orquestração customizada por código.

---

## 7. Summary e auditoria

Independente de master ou `for_each_task`, **todas as execuções ficam em `ctrl_ingestion_runs`** com `master_run_id`/`run_group_id` propagados.

### 7.1 Notebook `summary`

```python
dbutils.widgets.text("master_run_id", "")
master_run_id = dbutils.widgets.get("master_run_id")

if not master_run_id:
    raise ValueError("master_run_id obrigatório")

# Visão por tabela
detail = spark.sql(f"""
    SELECT layer, target_table, status, mode,
           rows_read, rows_written, rows_quarantined,
           duration_seconds, write_committed,
           error_message
    FROM ops.ctrl_ingestion_runs
    WHERE master_run_id = '{master_run_id}'
    ORDER BY layer, started_at_utc
""")
detail.display()

# Visão agregada por layer
agg = spark.sql(f"""
    SELECT
        layer,
        sum(case when status = 'SUCCESS'  then 1 else 0 end) as ok,
        sum(case when status = 'FAILED'   then 1 else 0 end) as fail,
        sum(case when status = 'DRY_RUN'  then 1 else 0 end) as dry,
        sum(rows_written) as rows_written,
        sum(rows_quarantined) as rows_quarantined,
        round(sum(duration_seconds), 1) as duration_s
    FROM ops.ctrl_ingestion_runs
    WHERE master_run_id = '{master_run_id}'
    GROUP BY layer
    ORDER BY layer
""")
agg.display()

# Quality issues
quality = spark.sql(f"""
    SELECT q.target_table, q.rule_name, q.failed_count, q.details_json
    FROM ops.ctrl_ingestion_quality q
    JOIN ops.ctrl_ingestion_runs r USING (run_id)
    WHERE r.master_run_id = '{master_run_id}'
    ORDER BY q.failed_count DESC
""")
quality.display()

# Falhas (para alerta)
failures = detail.filter("status = 'FAILED'").collect()
if failures:
    msg = "\n".join(
        f"- {f.target_table}: {(f.error_message or '')[:200]}"
        for f in failures
    )
    # exemplo de envio: webhook Slack, e-mail, etc.
    print(f"FALHAS:\n{msg}")
    raise RuntimeError(f"{len(failures)} ingestões falharam")
```

### 7.2 Dashboard SQL

Crie um dashboard Databricks SQL apontando para essas queries com filtro por dia:

```sql
SELECT layer, target_table, status,
       rows_written, duration_seconds,
       run_id, started_at_utc
FROM ops.ctrl_ingestion_runs
WHERE run_date = current_date()
ORDER BY started_at_utc DESC
```

### 7.3 Alertas

Databricks SQL Alerts em cima de:

```sql
SELECT count(*) as failures
FROM ops.ctrl_ingestion_runs
WHERE run_date = current_date() AND status = 'FAILED'
```

Threshold > 0 → notifica.

---

## 8. Testando localmente sem Databricks

Você pode executar o framework end-to-end na sua máquina (ou em CI) usando PySpark + Delta locais.

### 8.1 Setup

```bash
# Java 17 instalado (Linux/macOS via brew, Windows via SDK)
java -version

pip install -e ".[dev]"
# O extra dev já inclui PySpark/Delta para a suite completa.
```

### 8.2 Script de teste local

```python
# scripts/run_local.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("lakehouse-local")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "./_warehouse")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# crie schemas locais
for db in ["bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

# DF de teste
df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Carol")],
    "id long, nome string"
)

from lakehouse_ingestion import ingest

result = ingest(
    source=df,
    target_table="b_clientes_local",
    catalog="spark_catalog",
    layer="bronze",
    mode="scd0_append",
    notebook_name="run_local",
)
print(result)

spark.table("spark_catalog.bronze.b_clientes_local").show()
```

```bash
python scripts/run_local.py
```

Output esperado:

```
{'status': 'SUCCESS', 'rows_written': 3, ...}
+---+-----+-------------+-------------+---------+
|id |nome |ingestion_date|source_system|__run_id |
+---+-----+--------------+-------------+---------+
|1  |Alice|2026-05-09    |default      |...      |
...
```

### 8.3 Em CI (GitHub Actions)

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: python -m pip install -e ".[dev]"
      - run: python scripts/check_release.py
      - run: ruff check .
      - run: pytest -q
        env:
          SKIP_SPARK_TESTS: "1"

  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: python -m pip install build twine
      - run: python -m build
      - run: twine check dist/*
```

---

## 9. Troubleshooting

### "RuntimeError: Nenhuma SparkSession ativa encontrada"

Você está rodando fora de Databricks e nenhuma sessão foi criada. Solução: crie a sessão **antes** de chamar `ingest()`:

```python
from pyspark.sql import SparkSession
SparkSession.builder.master("local[2]").getOrCreate()
from lakehouse_ingestion import ingest  # agora resolve a sessão
```

### "ModuleNotFoundError: No module named 'delta'"

Falta `delta-spark`. Fora do Databricks, instale o extra Spark:

```bash
pip install "contractforge[spark]"
```

Em Databricks, isso já vem com o runtime — esse erro só aparece localmente.

### "ConcurrentAppendException" / "Conflicting concurrent commit"

Dois jobs escrevendo na mesma tabela ao mesmo tempo. O framework já retenta automaticamente (3 vezes por padrão). Se persistir:

- Use `lock_enabled=True` no plan para reduzir colisão (best-effort).
- Particione melhor a tabela target (cada job escreve em partição própria).
- Reduza paralelismo do `for_each_task` (`concurrency: 1` para testar).
- Use `merge_strategy="delta_by_partition"` para escopo mais estreito.
- Para `merge_strategy="replace_partitions"`, informe `merge_partition_column` e `replace_partitions_source_complete=True` somente quando o source contiver o estado completo das partições afetadas.

### "Schema policy strict violada"

Um schema mudou em relação ao target. Opções:

- Se a mudança é desejada: troque para `additive_only` ou `permissive`.
- Se não é: investigue o source. O erro mostra `added`, `removed`, `type_changes`.

### "quality.accepted_values.X possui N valores. Use uma tabela de referência"

Lista de valores aceitos > 1000. Solução: mantenha a referência numa tabela e valide via `LEFT ANTI JOIN`:

```python
# antes do ingest, exclua linhas inválidas
df_clean = df.join(
    spark.table("ref.tipos_validos"), on="tipo", how="leftsemi"
)
```

### "DRY_RUN" sem escrever

`dry_run=True` está no plan. Remova ou passe `False` para escrever de verdade.

### Watermark não avança

- Confirme que `watermark_columns` aponta para colunas existentes no DataFrame **e** não nulas.
- Após escrita com sucesso, confira:
  ```sql
  SELECT watermark_value FROM ops.ctrl_ingestion_state
  WHERE target_table = 'cat.silver.tabela'
  ```
- Em falha, watermark **não avança** (intencional). Resolva a falha primeiro.

### "Bronze deve ser orientada a captura"

Você tentou `scd1_upsert`/`scd2_historical`/`snapshot_soft_delete` em layer bronze. Bronze só aceita `scd0_append`, `scd0_overwrite`, `scd1_hash_diff`. Mude o `mode` ou a `layer`.

### "snapshot_soft_delete exige snapshot completo"

`snapshot_soft_delete` **não aceita** `watermark_columns` nem `filter_expression`: o motor marca como `is_active=false` qualquer chave do target ausente do source, e um source filtrado faria todas as linhas fora do filtro virarem inativas. Para sincronização incremental, troque para `scd1_upsert`. Para snapshot, garanta que a fonte é o estado-fim completo.

### "Regras abortivas {...} não são quarentenáveis"

`unique_key`, `min_rows` e `required_columns` descrevem propriedades do conjunto. Quando alguma falha com `on_quality_fail="quarantine"`, o framework escala para `fail` e aborta — o oposto seria escrever o dataset inteiro com `status=FAILED`. Para tolerar, use `on_quality_fail="warn"`. Para isolar duplicatas, faça `dedup_order_expr` antes do quality gate.

---

## 10. Checklist pré-produção

Antes de subir um pipeline novo:

- [ ] **Pacote** instalado no cluster (verificou `import lakehouse_ingestion; print(__version__)`).
- [ ] **Catálogo `ops`** existe e o cluster tem `CREATE TABLE` lá.
- [ ] Cada YAML tem `notebook_name` único e descritivo (vai aparecer em logs e OpenLineage).
- [ ] Cada YAML tem `description`, `owner`, `domain`, `tags` e `sla` quando houver governança mínima.
- [ ] `merge_keys` / `hash_keys` estão corretos (consultou amostras com duplicatas).
- [ ] `quality_rules` tem ao menos `not_null` nas chaves.
- [ ] `schema_policy` definida e `allow_type_widening` só habilitado quando a evolução de tipos foi intencional.
- [ ] Para SCD2: `scd2_change_columns` é o conjunto **mínimo** que define mudança real.
- [ ] Para snapshot: source é **realmente** completo (sem watermark).
- [ ] `dry_run=True` rodou ao menos uma vez e o resultado foi inspecionado.
- [ ] Suite de testes passa (`pytest`).
- [ ] Job tem `retry` configurado (≥1 retry com 30s).
- [ ] Summary task ou alerta SQL configurado para `FAILED`.
- [ ] Para `for_each_task`: `concurrency` definido (não deixar default).
- [ ] OpenLineage habilitado se há collector externo.

---

## 11. FAQ

**P: Posso misturar pacote e script no mesmo Workspace?**
Sim. O pacote no cluster + um notebook com `%run` legacy convivem. Mas evite — versões podem divergir e gerar bugs sutis.

**P: Como atualizo o pacote num cluster sem reiniciá-lo?**
Não dá com cluster libraries — exige restart. Para atualização sem restart, use `%pip install --upgrade` notebook-scoped.

**P: O framework suporta Streaming?**
Suporta apenas Autoloader com `trigger: available_now`. Streaming contínuo (`processingTime`/`continuous`) continua fora do escopo; para esse caso, considere DLT/Lakeflow.

**P: Posso usar minhas próprias tabelas de controle?**
Os nomes vêm de `FrameworkConfig.ctrl_table_*`. Para customizar, monkey-patch o `CONFIG` antes de qualquer chamada (não recomendado em produção). O caminho oficial é via `ctrl_schema` no plan.

**P: Como removo dados antigos das ctrl tables?**
`ctrl_ingestion_runs` é particionada por `run_date` — `DELETE` por partição é eficiente:

```sql
DELETE FROM ops.ctrl_ingestion_runs WHERE run_date < current_date() - 90;
VACUUM ops.ctrl_ingestion_runs RETAIN 168 HOURS;
```

**P: O master_run_id e o {{job.run_id}} são a mesma coisa?**
Não, mas costumamos usar o segundo como valor do primeiro (vide YAMLs). `{{job.run_id}}` é numérico e único por job-run; `master_run_id` no framework é uma coluna STRING que aceita qualquer valor — UUID, run_id do Databricks, ou correlation id externo.

**P: `for_each_task` aceita quantas iterações?**
Limites do Databricks: até 1000 iterações por task. Para mais, divida em múltiplos `for_each_task`.

**P: Como testar um YAML sem rodar de fato?**
`dry_run: true` no YAML. O `ingest_plan` faz validação completa (schema, quality, watermark) e retorna `status="DRY_RUN"` sem escrever. Importante: a partir da versão atual, `dry_run` é **realmente** sem efeitos colaterais — não cria schemas/ctrl tables, não aplica `ALTER TABLE ADD COLUMNS`, não persiste linhas em `ctrl_ingestion_quality`/`quarantine`/`runs`/`state`/`lineage`. É seguro rodar contra ambientes de produção só para validar o plano.

**P: `on_quality_fail="quarantine"` realmente isola todas as falhas?**
Não. Apenas as regras de linha (`not_null`, `accepted_values`, `max_null_ratio`) podem ser quarentenadas. Regras de conjunto (`unique_key`, `min_rows`, `required_columns`) descrevem propriedades agregadas e não conseguem isolar linhas. Se uma dessas falhar, o framework escala automaticamente para `fail` e aborta a execução — preferindo erro explícito a escrita silenciosa de dados duvidosos.

**P: Posso forçar o framework a usar uma SparkSession específica em testes?**
Sim. Em `tests/conftest.py` já fazemos:

```python
from lakehouse_ingestion import _spark as spark_module
spark_module._cached_session = sess
```

**P: Quais permissões UC o cluster precisa?**
- `USE CATALOG` no catálogo alvo.
- `USE SCHEMA` + `CREATE TABLE` em `<catalog>.<layer>` (bronze/silver/gold).
- `USE SCHEMA` + `CREATE TABLE` em `<catalog>.<ctrl_schema>` (default: `ops`).
- `MODIFY` nas tabelas existentes do alvo.
- `READ` nas fontes.

---

**Fim do guia.**
Reportar problemas ou sugerir melhorias: abra issue em https://github.com/marquesantero/contractforge.
