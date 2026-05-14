# ContractForge

Framework de ingestão para Databricks e Delta Lake, com contratos declarativos por tabela, suporte a Bronze/Silver/Gold, quality gates, watermarks tipados, SCD, snapshot com soft delete, explain mode e emissão de eventos OpenLineage em JSON.

Documentação:
- [docs/guia_de_uso.md](docs/guia_de_uso.md) — passo a passo prático para testar como pacote ou script, padrão YAML + notebook genérico, orquestração com `for_each_task` e com master, troubleshooting e FAQ.
- [docs/arquitetura.md](docs/arquitetura.md) — referência técnica detalhada de cada submódulo, fluxo de execução, esquemas das ctrl tables e decisões de design (~70 KB).
- [docs/oficial.md](docs/oficial.md) — documentação oficial completa de uso, contratos, modos, observabilidade e extensão.
- [docs/adrs/README.md](docs/adrs/README.md) — decisões arquiteturais registradas como ADRs.
- [CHANGELOG.md](CHANGELOG.md) — histórico de versões e política de release.

## Posicionamento

O framework é uma biblioteca **contract-first** para ingestão e governança declarativa em Lakehouse. O contrato não descreve apenas a escrita: ele concentra regras de qualidade, schema policy, metadata de catálogo, operações, observabilidade e governança de acesso em artefatos versionáveis.

Ele não substitui Delta Live Tables/Lakeflow. O objetivo é outro: oferecer controle fino, contratos revisáveis por tabela e portabilidade para jobs, notebooks, DAB e runtimes Spark/Delta compatíveis. Em ambientes Databricks, ele complementa Unity Catalog aplicando comments/tags e produzindo evidências operacionais em ctrl tables.

Principais diferenciais:

- Separação por responsabilidade: `*.ingestion.yaml`, `*.annotations.yaml`, `*.operations.yaml` e `*.access.yaml`.
- Modos de escrita explícitos para Medallion, incluindo SCD1, SCD2, hash-diff e `snapshot_soft_delete`.
- Observabilidade persistente em Delta: runs, qualidade, quarentena, erros, lineage, streaming, annotations, operations e access.
- API defensiva para combinações perigosas, como `snapshot_soft_delete` com watermark/filter.

## Instalação local

O produto e o pacote distribuído se chamam **ContractForge** (`contractforge`). O namespace Python permanece `lakehouse_ingestion`, então o uso programático continua com `from lakehouse_ingestion import ...`.

```bash
pip install .
```

No Databricks, o wheel não declara `pyspark` nem `delta-spark` como dependências obrigatórias, porque Spark e Delta já são fornecidos pelo runtime. Para desenvolvimento local fora do Databricks, instale o extra `spark`:

```bash
pip install ".[spark]"
```

Para desenvolvimento e testes:

```bash
pip install -e ".[dev]"
```

## Build para PyPI

```bash
python -m pip install build twine
python scripts/check_release.py
python -m build
twine check dist/*
```

## Requisitos de runtime

- Python 3.10+
- PySpark 3.4 até 3.5.x e delta-spark 3.0 até 3.x quando rodando fora do Databricks (`pip install ".[spark]"`)
- Databricks Runtime equivalente quando rodando em cluster clássico ou serverless
- Uma SparkSession ativa antes da chamada de `ingest()`. O framework resolve a sessão por:
  1. `databricks.sdk.runtime.spark` quando rodando em Databricks
  2. `SparkSession.getActiveSession()` em qualquer outro ambiente
  3. Erro explícito se nenhuma sessão estiver ativa

## Exemplo

```python
from lakehouse_ingestion import ingest

result = ingest(
    source=df,
    target_table="c_cliente",
    catalog="sandbox_catalog1",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    column_mapping={"src_cliente_id": "id_cliente"},
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    delta_properties={"delta.enableChangeDataFeed": "true"},
    retry_attempts=5,
    retry_backoff_seconds=10,
    schema_policy="additive_only",
    quality_rules={"not_null": ["id_cliente"], "unique_key": ["id_cliente"]},
    explain_mode=True,
    openlineage_enabled=True,
)
```

## Fontes e Conectores Declarativos

Além de tabela e DataFrame, `source` aceita fontes declarativas. Use `SourceSpec` para o formato legado de Auto Loader ou `ConnectorSpec` (`source.type=connector`) para o modelo unificado de conectores.

Conectores nativos:

- Catálogo/SQL: `table`, `delta_table`, `view`, `sql`.
- Arquivos: `parquet`, `json`, `csv`, `text`.
- Object storage/blob: `object_storage`, `blob` com `provider=adls|azure_blob|s3|gcs`.
- Sistemas externos: `jdbc`, `rest_api`.
- Streaming finito: `autoloader` com `trigger=available_now`.

Exemplo com Auto Loader no formato unificado:

```python
result = ingest(
    source={
        "type": "connector",
        "connector": "autoloader",
        "path": "/Volumes/main/raw/orders",
        "format": "parquet",
        "read": {
            "schema_location": "/Volumes/main/ops/schemas/orders",
            "checkpoint_location": "/Volumes/main/ops/checkpoints/orders",
            "include_existing_files": True,
        },
    },
    target_table="b_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    idempotency_key="orders-2026-05-12",
    idempotency_policy="skip_if_success",
)
```

O stream usa `foreachBatch` e cada batch chama `ingest_plan` internamente. A execução externa é registrada em `ctrl_ingestion_streams`; os runs filhos ficam em `ctrl_ingestion_runs` com `parent_run_id = stream_run_id`.

Escopo intencional: apenas Autoloader `available_now`. Streaming contínuo (`processingTime`/`continuous`) continua fora da lib.

Exemplo com REST API paginada:

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
schema_policy: additive_only
```

Exemplo com JDBC:

```yaml
source:
  type: connector
  connector: jdbc
  name: erp_orders
  options:
    url: "{{ secret:erp/jdbc_url }}"
    dbtable: public.orders
    user: "{{ secret:erp/user }}"
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

`source.incremental` permite pushdown do watermark anterior para a origem sem mudar o controle de watermark do framework. Em REST, use `watermark_param`, `watermark_header` ou `watermark_body_field`; em JDBC, use `watermark_column` ou `predicate`. `initial_value` é usado apenas na primeira execução, quando ainda não há watermark salvo. A configuração incremental é auditada em `ctrl_ingestion_runs.source_incremental_json`.

Cada execução com conector também grava observabilidade específica em `ctrl_ingestion_runs.source_metrics_json`. Para REST são registrados `request_count`, `pages_read`, `records_read`, `bytes_read`, paginação, retry, rate limit e watermark aplicado. Para JDBC são registrados estratégia de leitura, uso de incrementalidade, watermark aplicado, particionamento e `fetchsize`. Para arquivos/tabelas/SQL são registrados estratégia de leitura e sinalização de fonte completa.

`source.read.source_complete=true` ou `full_snapshot=true` é a declaração explícita usada por modos que exigem fonte completa, como `snapshot_soft_delete` e `replace_partitions`. Credenciais com `{{ secret:scope/key }}` são resolvidas via Databricks Secrets ou variável de ambiente `CONTRACTFORGE_SECRET_SCOPE_KEY`; logs e ctrl tables recebem versões redigidas.

## Contrato declarativo

- `preset` aplica defaults opinativos para padrões comuns de ingestão; o contrato explícito sempre vence o preset.
- `column_mapping` renomeia colunas source -> target antes de filtros, watermarks, quality e escrita. Destinos duplicados, colisões com colunas existentes e nomes técnicos reservados são rejeitados.
- `shape` transforma estruturas JSON/struct/array antes de filtros, watermarks, quality e escrita.
- `delta_properties` aplica `TBLPROPERTIES` na criação da tabela Delta, por exemplo `delta.enableChangeDataFeed`, `delta.autoOptimize.optimizeWrite` ou propriedades de retenção.
- `retry_attempts` e `retry_backoff_seconds` sobrescrevem a política global de retry por plano.
- `annotations`, `operations` e `access` podem ficar no próprio contrato ou em YAMLs separados (`*.annotations.yaml`, `*.operations.yaml`, `*.access.yaml`) carregados por `load_contract_bundle()`/`ingest_bundle()`.
- A origem não pode trazer colunas técnicas gerenciadas pelo framework (`ingestion_date`, `ingestion_ts_utc`, `source_system`, `__run_id`, `row_hash`, etc.), evitando sobrescrita silenciosa.
- Modos baseados em `MERGE` abortam se todas as `merge_keys` vierem nulas e emitem warning quando houver nulos parciais.

### Shape para JSON, structs e arrays

Use `shape` para normalizar JSON bruto em colunas analíticas, especialmente em Silver:

```yaml
preset: silver_scd1_upsert
source: bronze.raw_orders
target_table: s_orders_items
catalog: main
merge_keys: order_item_key

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
  flatten:
    enabled: true
    include: [customer]
    separator: "_"
```

Arrays podem ser declarados em qualquer ordem; a lib resolve dependências por path/alias. Arrays irmãos com `explode` são bloqueados por padrão para evitar produto cartesiano:

```yaml
shape:
  arrays:
    - path: items
      mode: explode_outer
      alias: item
    - path: payments
      mode: explode_outer
      alias: payment
      allow_cartesian: true  # declare apenas se a multiplicação for intencional
```

Em Bronze, `explode`/`explode_outer` é bloqueado por padrão para preservar a evidência bruta. Se for intencional:

```yaml
shape:
  allow_cardinality_change_on_bronze: true
  arrays:
    - path: items
      mode: explode_outer
      alias: item
```

### Contratos separados de governança

Use arquivos separados quando engenharia, governança, operações e segurança tiverem ciclos de revisão diferentes:

```text
contracts/gold/gd_orders.ingestion.yaml
contracts/gold/gd_orders.annotations.yaml
contracts/gold/gd_orders.operations.yaml
contracts/gold/gd_orders.access.yaml
```

`annotations` aplica comments e tags de tabela/coluna, incluindo aliases, PII e depreciação. `operations` registra ownership estruturado, criticidade, SLA, grupos e runbook para dashboards externos. `access` declara grants, row filters e column masks.
`ingest_plan` aplica `operations`/`annotations` e deixa `access` como `DEFERRED`; permissões rodam pelo comando dedicado `apply-access`. `governance-check` compara grants declarados com `SHOW GRANTS ON TABLE`. `revoke_unmanaged=true` é operação perigosa e só executa `REVOKE` via `apply-access --force-revoke`.
`access_policy.on_drift=fail` bloqueia aplicação quando houver drift; `warn` apenas sinaliza; `reconcile` permite correção declarativa e revogação só com `revoke_unmanaged=true` + `--force-revoke`.

```python
from lakehouse_ingestion import ingest_bundle

result = ingest_bundle("contracts/gold/gd_orders")
```

## Presets

Presets reduzem repetição sem esconder decisões críticas. Eles podem ser usados isolados ou combinados com modificadores:

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
```

Presets de ingestão disponíveis:

- Bronze: `bronze_autoloader_append`, `bronze_file_append`, `bronze_table_append`, `bronze_full_overwrite`, `bronze_partition_overwrite`.
- Silver: `silver_scd1_upsert`, `silver_scd1_partition_upsert`, `silver_replace_partitions`, `silver_hash_diff_append`, `silver_snapshot_soft_delete`, `silver_scd2_historical`, `silver_incremental_watermark_upsert`, `silver_quarantine_ingestion`.
- Gold: `gold_full_refresh`, `gold_partition_refresh`, `gold_replace_partitions`, `gold_snapshot_serving`, `gold_scd1_serving`.
- Modificadores: `quality_strict`, `quality_quarantine`, `delta_cdf_enabled`, `delta_optimized_writes`, `runtime_databricks_serverless`, `runtime_spark_delta_local`, `governance_uc_basic`.

Comandos úteis:

```bash
contractforge presets list
contractforge presets show silver_scd1_upsert
contractforge connectors list
contractforge connectors show rest_api jdbc
contractforge validate contracts/silver/orders.yaml --expand-presets
```

Extensão programática:

```python
from lakehouse_ingestion import register_preset

register_preset("company_silver_default", {
    "layer": "silver",
    "mode": "scd1_upsert",
    "schema_policy": "additive_only",
    "on_quality_fail": "quarantine",
})
```

Validação local sem Spark:

```bash
contractforge validate-bundle contracts/gold/gd_orders
contractforge governance-preview contracts/gold/gd_orders
contractforge governance-check contracts/gold/gd_orders
contractforge drift-check contracts/gold/gd_orders
contractforge governance-apply contracts/gold/gd_orders
contractforge apply-annotations contracts/gold/gd_orders
contractforge validate-access contracts/gold/gd_orders
contractforge apply-access contracts/gold/gd_orders
contractforge apply-access contracts/gold/gd_orders --force-revoke
```

## Modos oficiais

- `scd0_append`: inserção imutável.
- `scd0_overwrite`: substituição total ou por partição.
- `scd1_upsert`: atualização do estado atual por chaves.
- `scd1_upsert` com `merge_strategy="replace_partitions"` exige `merge_partition_column` e `replace_partitions_source_complete=True`, pois sobrescreve as partições afetadas.
- `scd1_hash_diff`: inserção apenas de versões novas ou alteradas por hash. O framework mantém `ingestion_ts_utc` como coluna técnica para ordenar o último estado quando `dedup_order_expr` não é informado.
- `scd2_historical`: histórico completo com `valid_from`, `valid_to` e `is_current`. Reaparições de chaves não correntes criam uma nova versão atual.
- `snapshot_soft_delete`: sincronização por snapshot com marcação de ausentes em `is_active` e `deleted_at`. Exige source completo — o framework rejeita com `ValueError` quando combinado com `watermark_columns` ou `filter_expression`. Usa `MERGE` SQL em todos os runtimes para manter comportamento consistente entre classic e serverless.

## Quality gates

Definidas via parâmetro `quality_rules` (dict ou `QualityRules`):

- `required_columns`, `not_null`, `unique_key`, `accepted_values`, `min_rows`, `max_null_ratio`, `expressions`.
- A avaliação consolida regras de coluna e `expressions` numa única agregação para reduzir I/O em datasets grandes.
- A ação em falha (`on_quality_fail`) pode ser:
  - `fail` (padrão): aborta a execução.
  - `warn`: registra mas escreve tudo.
  - `quarantine`: linhas problemáticas vão para `ctrl_ingestion_quarantine`; o restante é gravado e `effective_rows = rows_read - rows_quarantined`. **Vale apenas para regras de linha** (`not_null`, `accepted_values`, `max_null_ratio`). Regras de conjunto (`unique_key`, `min_rows`, `required_columns`) não têm como isolar linhas e escalam automaticamente para `fail`.

Exemplo de regra complexa aditiva:

```python
quality_rules={
    "not_null": ["order_id"],
    "expressions": [
        {"name": "positive_amount", "expression": "amount > 0", "severity": "quarantine"},
        {
            "name": "valid_period",
            "expression": "end_date >= start_date",
            "severity": "abort",
            "message": "Período inválido.",
        },
    ],
}
```

## Schema policy

- `permissive`: permite adições e remoções; mudanças de tipo inseguras continuam bloqueadas.
- `additive_only`: aceita colunas novas, rejeita remoções e mudanças de tipo inseguras.
- `strict`: rejeita qualquer divergência.

Em `permissive` e `additive_only`, colunas novas são adicionadas ao Delta target via `ALTER TABLE`.

Para evoluções seguras de tipo, use `allow_type_widening=True`. O framework valida alargamentos simples (`int -> bigint`, `float -> double`, aumento de precisão decimal, `date -> timestamp`), aplica `ALTER COLUMN TYPE` quando suportado pelo Delta e registra as mudanças em `ctrl_ingestion_schema_changes`.

## Observabilidade

O framework cria tabelas de controle no schema configurado:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_state`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_locks`
- `ctrl_ingestion_explain`
- `ctrl_ingestion_lineage`
- `ctrl_ingestion_errors`
- `ctrl_ingestion_metadata`
- `ctrl_ingestion_schema_changes`
- `ctrl_ingestion_streams`
- `ctrl_ingestion_annotations`
- `ctrl_ingestion_operations`
- `ctrl_ingestion_access`

`explain_mode=True` captura o plano Spark do DataFrame preparado.

`openlineage_enabled=True` grava um evento OpenLineage em JSON na tabela de lineage.

Em falha, `ctrl_ingestion_runs.error_message` guarda uma mensagem curta para consulta rápida e `ctrl_ingestion_errors.stack_trace` guarda o traceback completo.

`idempotency_key` permite identificar um lote lógico. Use `idempotency_policy` para controlar reexecuções: `always_run`, `skip_if_success`, `fail_if_success` ou `rerun_if_failed`.

O retorno preserva `rows_written` como métrica lógica da biblioteca, expõe `rows_inserted`, `rows_updated`, `rows_deleted`, `stage_durations`, `contract_metadata` e inclui `metrics_source`, `framework_version`, `ctrl_schema_version`, `runtime_type`, `spark_version` e `python_version`:

- `logical`: apenas contadores calculados pela biblioteca.
- `mixed`: contadores lógicos com evidência adicional do histórico Delta.

## Extensibilidade e DX

- `IngestionHooks` permite callbacks programáticos `before_read`, `after_prepare`, `before_write` e `after_write`. Hooks que recebem DataFrame devem retornar DataFrame.
- `register_write_mode(mode, handler)` registra motores de escrita customizados quando houver necessidade real de extensão.
- `register_quality_rule(type, evaluator)` registra regras customizadas usadas por `quality_rules.custom`. Regras custom com `severity="quarantine"` devem retornar uma condição de linha.
- `register_source_resolver(name, resolver)` registra conectores customizados. O contrato aceita qualquer `source.connector` com nome válido; a execução falha cedo se não houver resolver registrado.
- `yaml_schema()` retorna o JSON Schema do contrato para autocomplete/validação em IDEs.
- A CLI `contractforge validate contrato.yaml` valida contratos YAML/JSON sem executar Spark e aplica validação estática dos conectores nativos. `contractforge schema` imprime o schema.
- `contractforge connectors list|show` exibe conectores registrados, campos obrigatórios e capabilities.

## Matriz de runtime

| Modo | Classic | Serverless / Spark Connect |
|---|---:|---:|
| `scd0_append` | suportado | suportado |
| `scd0_overwrite` | suportado | suportado |
| `scd1_upsert` | suportado | suportado via SQL `MERGE` |
| `scd1_hash_diff` | suportado | suportado |
| `scd2_historical` | suportado | suportado via SQL `MERGE` |
| `snapshot_soft_delete` | suportado | suportado via SQL `MERGE` |

## Estrutura do pacote

```
src/lakehouse_ingestion/
├── __init__.py        # Façade pública (ingest, ingest_plan, IngestionPlan, QualityRules, FrameworkConfig)
├── cli.py             # CLI contractforge validate/schema
├── contract_schema.py # JSON Schema do contrato declarativo
├── hooks.py           # IngestionHooks
├── _spark.py          # Resolução de SparkSession + safe_cache/serverless
├── _sql.py            # Helpers de identificadores e literais SQL
├── config.py          # FrameworkConfig, tipos e constantes
├── plan.py            # IngestionPlan, QualityRules, build_plan_from_kwargs
├── presets.py         # presets declarativos e registry de presets customizados
├── shape.py           # transformações declarativas para JSON, structs e arrays
├── sources.py         # Source resolvers declarativos (Autoloader available_now)
├── schema.py          # hash, dedup, encoding, schema policy
├── watermark.py       # watermark simples e composto, encode/decode/apply
├── quality.py         # quality gates (avaliação consolidada) + quarentena
├── state.py           # ctrl tables, log, upsert state, locks, retry
├── writers.py         # motores por modo (append, overwrite, upsert, hash diff, snapshot, scd2)
├── lineage.py         # explain capture e OpenLineage
└── ingestion.py       # orquestrador (ingest, ingest_plan)
```

Decisões arquiteturais formais ficam em [docs/adrs](docs/adrs/README.md).

## Testes

```bash
pip install -e ".[dev]"
pytest
```

A suíte tem dois grupos:

- **Testes puros** (rápidos, sem Spark): validações de plano e parsing.
- **Testes integrados com Spark + Delta**: 6 modos de escrita, quality gates, watermark, schema policy, sources e streaming `available_now`.

Status validado localmente: `152 passed` com Python 3.11, PySpark 3.5.x, delta-spark 3.x e Java disponível.

Variável `SKIP_SPARK_TESTS=1` força o pulo dos testes integrados.
