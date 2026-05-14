# ContractForge — Arquitetura e Referência Técnica

**Versão do pacote:** `1.15.0`
**Pacote Python:** `contractforge`
**Import principal:** `lakehouse_ingestion`
**Ambiente-alvo:** Databricks Runtime, Unity Catalog, Delta Lake (também roda em PySpark + delta-spark fora do Databricks)
**Licença:** MIT

> Este documento é a referência **técnica** do pacote: descreve cada submódulo, contrato de função, fluxo de execução, edge cases e decisões de design. Para um guia voltado ao **uso**, ver [oficial.md](./oficial.md). Decisões arquiteturais formais ficam em [adrs/](./adrs/README.md).

---

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Estrutura do pacote](#2-estrutura-do-pacote)
3. [Fluxo de execução de uma chamada `ingest()`](#3-fluxo-de-execução-de-uma-chamada-ingest)
4. [Submódulos em detalhe](#4-submódulos-em-detalhe)
   - [4.1 `_spark.py` — Resolução de SparkSession](#41-_sparkpy--resolução-de-sparksession)
   - [4.2 `_sql.py` — Helpers de SQL](#42-_sqlpy--helpers-de-sql)
   - [4.3 `config.py` — Configuração e tipos](#43-configpy--configuração-e-tipos)
   - [4.4 `plan.py` — Contrato declarativo](#44-planpy--contrato-declarativo)
   - [4.5 `presets.py` — Defaults declarativos acopláveis](#45-presetspy--defaults-declarativos-acopláveis)
   - [4.6 `shape.py` — Transformações JSON/struct/array](#46-shapepy--transformações-jsonstructarray)
   - [4.7 `schema.py` — Hash, dedup, encoding e schema policy](#47-schemapy--hash-dedup-encoding-e-schema-policy)
   - [4.8 `watermark.py` — Watermark tipado](#48-watermarkpy--watermark-tipado)
   - [4.9 `quality.py` — Quality gates e quarentena](#49-qualitypy--quality-gates-e-quarentena)
   - [4.10 `state.py` — Tabelas de controle, log, lock, retry](#410-statepy--tabelas-de-controle-log-lock-retry)
   - [4.11 `writers.py` — Motores de escrita](#411-writerspy--motores-de-escrita)
   - [4.12 `lineage.py` — Explain e OpenLineage](#412-lineagepy--explain-e-openlineage)
   - [4.13 `ingestion.py` — Orquestrador](#413-ingestionpy--orquestrador)
5. [Modos de escrita — semântica e garantias](#5-modos-de-escrita--semântica-e-garantias)
6. [Quality gates — avaliação consolidada](#6-quality-gates--avaliação-consolidada)
7. [Schema policy — políticas e ALTER automático](#7-schema-policy--políticas-e-alter-automático)
8. [Watermarks — encoding, aplicação e estado](#8-watermarks--encoding-aplicação-e-estado)
9. [Tabelas de controle — esquemas e papéis](#9-tabelas-de-controle--esquemas-e-papéis)
10. [Locks, retry e idempotência](#10-locks-retry-e-idempotência)
11. [Lineage OpenLineage e Explain](#11-lineage-openlineage-e-explain)
12. [Configuração e parâmetros](#12-configuração-e-parâmetros)
13. [Testes](#13-testes)
14. [Deploy e empacotamento](#14-deploy-e-empacotamento)
15. [Decisões de design](#15-decisões-de-design)
16. [Glossário](#16-glossário)

---

## 1. Visão geral

### 1.1 Propósito

O `contractforge` padroniza a ingestão de dados em Delta Lake fornecendo:

- Um **contrato declarativo** (`IngestionPlan`) por tabela, em vez de scripts ad-hoc.
- **Seis modos oficiais de escrita** cobrindo append imutável, overwrite, SCD1, SCD2 histórico, hash-diff e snapshot com soft delete.
- **Quality gates** com três modos de falha (`fail`, `warn`, `quarantine`).
- **Watermarks tipados** (simples e compostos) persistidos em tabela de estado.
- **Schema policy** com três níveis (`permissive`, `additive_only`, `strict`), evolução aditiva e alargamento seguro opcional de tipos.
- **Observabilidade**: tabelas de controle para runs, state, quality, quarantine, locks, explain, lineage, errors, metadata e schema changes.
- **Lineage OpenLineage** (1.0.5) e captura de plano Spark.
- **Idempotência operacional** via locks best-effort, `with_retry` para conflitos Delta e MERGE atômico.

### 1.2 O que ele NÃO faz

- **Não orquestra**. O framework roda dentro de um job ou notebook; agendamento fica com Workflows, Airflow, DAB, etc.
- **Não substitui DLT**. É funcional, batch, declarativo por tabela; não mantém grafo de dependências entre tabelas.
- **Não é um catálogo de qualidade**. As regras de quality são simples e suficientes para gates de pipeline; expectations complexas exigem outra ferramenta.
- **Não cuida de descoberta de fontes**. Fontes são tabelas Unity Catalog ou DataFrames já montados.

### 1.3 API pública

A API pública re-exportada em `lakehouse_ingestion/__init__.py` inclui:

```python
from lakehouse_ingestion import (
    ingest,           # função procedural amigável a notebook
    ingest_plan,      # variante recebendo IngestionPlan
    IngestionPlan,    # dataclass do contrato
    QualityRules,     # dataclass das regras
    QualityExpression,# regra SQL declarativa com severidade
    IngestionHooks,   # callbacks programáticos controlados
    FrameworkConfig,  # dataclass de configuração global
    validate_plan_shape, # validação pura de contrato/YAML sem Spark
)
```

Todos os outros símbolos (`writers.*`, `quality.*`, `schema.*`, etc.) são considerados **internos** — podem mudar sem aviso entre minor versions. Se você precisar deles, abra uma issue antes.

---

## 2. Estrutura do pacote

```
lakehouse_ingestion_pkg/
├── pyproject.toml          # build (setuptools), deps, ruff, pytest
├── README.md               # guia rápido + estrutura do pacote
├── .gitignore              # build artifacts, venv, caches, derby.log
├── docs/
│   ├── oficial.md          # documentação oficial completa de uso
│   └── arquitetura.md      # ESTE arquivo (referência técnica)
├── src/
│   └── lakehouse_ingestion/
│       ├── __init__.py     # façade pública
│       ├── _spark.py       # resolução lazy de SparkSession + serverless
│       ├── _sql.py         # helpers de identificadores, literais, datas
│       ├── cli.py          # comandos contractforge validate/schema
│       ├── contract_schema.py # JSON Schema do contrato declarativo
│       ├── config.py       # FrameworkConfig + tipos (Layer, WriteMode, ...)
│       ├── hooks.py        # hooks opcionais de pré/pós-ingestão
│       ├── maintenance.py  # manutenção operacional de ctrl tables
│       ├── plan.py         # IngestionPlan, QualityRules, build_plan_from_kwargs
│       ├── presets.py      # presets declarativos e registry customizado
│       ├── shape.py        # shape declarativo para JSON, structs e arrays
│       ├── sources.py      # Source resolvers declarativos
│       ├── schema.py       # hash, dedup, custom keys, encoding, schema policy
│       ├── watermark.py    # encode/decode/apply/compute watermarks tipados
│       ├── quality.py      # evaluate_quality (single-pass) + quarentena
│       ├── state.py        # ctrl tables, log_run, upsert_state, locks, retry
│       ├── writers.py      # 6 motores de escrita + execute_write_mode
│       ├── lineage.py      # capture_explain, build/write_openlineage_event
│       └── ingestion.py    # orquestrador: ingest_plan, ingest
└── tests/
    ├── conftest.py         # fixture Spark + Delta com skip gracioso
    ├── test_plan.py        # 11 testes puros (sem Spark)
    ├── test_quality.py     # quality gates
    ├── test_watermark.py   # watermarks simples e compostos
    ├── test_schema.py      # hash, dedup, schema policy, table_exists
    └── test_modes.py       # 6 modos + dry run + bronze restriction + watermark
```

### 2.1 Camadas e dependências internas

```
   __init__.py
        │
        ▼
   ingestion.py  ← orquestrador
        │
        ├─→ plan.py ─┐
        ├─→ sources.py ─┐
        ├─→ hooks.py ───┤
        ├─→ writers.py ──┐
        ├─→ quality.py ──┤
        ├─→ state.py ────┤
        ├─→ schema.py ───┤
        ├─→ watermark.py ┤
        ├─→ lineage.py ──┘
        │       │
        │       ▼
        └──→ _spark.py, _sql.py, config.py
```

- `_spark.py`, `_sql.py`, `config.py` são **folhas**: não dependem de outros módulos do pacote.
- `plan.py` depende de `config.py`, `_sql.py`, `governance.py`, `hooks.py` e `presets.py`.
- `presets.py` depende só de `_sql.py` para normalizar listas de nomes.
- `shape.py` depende de Spark SQL functions/types e `_sql.py`; não depende do orquestrador.
- `sources.py` depende de `plan.py` e `_spark.py`.
- `hooks.py` é um contrato leve usado por `plan.py` e `ingestion.py`.
- `contract_schema.py` depende de constantes em `config.py`.
- `schema.py` depende de `_spark.py`, `_sql.py`, `config.py`.
- `watermark.py` depende de `_spark.py`, `_sql.py`, `schema.py` (para `table_exists`).
- `quality.py` depende de `plan.py` (`QualityRules`), `_spark.py`, `_sql.py`, `config.py`.
- `state.py` depende de `plan.py`, `_spark.py`, `_sql.py`, `config.py`.
- `writers.py` depende de `plan.py`, `schema.py`, `_spark.py`, `_sql.py`, `config.py`.
- `lineage.py` depende de `plan.py`, `_spark.py`, `_sql.py`.
- `ingestion.py` é o único módulo que importa todos os outros.

Não há ciclos. Removendo qualquer módulo "abaixo" de `ingestion.py` quebra apenas seu cone de dependentes.

---

## 3. Fluxo de execução de uma chamada `ingest()`

```
ingest(**kwargs)
    │
    ▼
build_plan_from_kwargs(**kwargs) ────► IngestionPlan (dataclass frozen)
    │
    ▼
ingest_plan(plan)
    │
    ▼
[1]  new_run_id, utc_now_str, today_str, utc_now_ts
[2]  target_full_table_name(plan)                   → target
[3]  ensure_ctrl_tables(catalog, ctrl_schema)        → tables{runs,state,...}
    │
    ▼   (try)
[4]  if plan.lock_enabled: acquire_lock(target, run_id)
[5]  raw_df, source_name = _resolve_source(plan)
[6]  wm_prev = get_watermark(state_table, target, plan.watermark_columns)
[7]  prepared_df = _prepare_dataframe(...)
       │ select → filter → custom_keys → apply_watermark → dedup → fix_encoding
       │ → withColumn(ingestion_date, source_system, __run_id)
[8]  prepared_df = safe_cache(prepared_df)
[9]  schema_changes = _validate_plan(plan, df, target)
       │ bronze/SCD2 rules → required cols → schema policy → ALTER ADD COLUMNS
[10] rows_read = prepared_df.count()
[11] wm_candidate = compute_watermark(df, plan.watermark_columns)
[12] if explain_mode: capture_explain → write_explain_plan
[13] (status, failed, valid_df, quarantined_df, q_count) = evaluate_quality(...)
       │ single-pass aggregation: not_null, accepted_values, max_null_ratio
       │ separated: unique_key (groupBy/count), required_columns (schema check)
[14] write_quality_results(...)
[15] if status == "FAILED":
       fail      → raise ValueError
       quarantine→ write_quarantine + prepared_df = valid_df
       warn      → log warning, continua
[16] if dry_run: return _build_dry_run_result(...)
[17] effective_rows = rows_read - q_count (se quarantine) ou rows_read
[18] delta_version_before = describe history limit 1
[19] write_started_at = now
[20] rows_written = with_retry(execute_write_mode(plan, df, target, effective))
       │   dispatcha p/ write_append / write_overwrite / write_upsert /
       │              write_scd1_hash_diff / write_snapshot_soft_delete /
       │              write_scd2
[21] write_finished_at = now
[22] delta_version_after = describe history limit 1
[23] write_committed = rows_written > 0 && version_before != version_after
[24] if optimize_after_write: run_optimize(target, zorder_columns)
[25] wm_current = compute_watermark(df, plan.watermark_columns)
[26] operation_metrics = describe history limit 1 (operationMetrics)
[27] row_metrics = extract_row_metrics(operation_metrics)
[28] upsert_state(SUCCESS, wm_current, run_id, rows_written)
    │
    ▼   (except Exception as exc)
       status = "FAILED"
       error  = traceback
       upsert_state(FAILED, wm_prev, error)
    │
    ▼   (finally)
[F1] safe_unpersist(prepared_df)
[F2] if plan.lock_enabled: release_lock(target, run_id)
[F3] _finalize_execution(...)  → log_run(tables, payload)
[F4] write_openlineage_event(...) (se openlineage_enabled)
    │
    ▼
return dict {status, run_id, rows_*, watermark_*, write_committed,
              delta_version_*, operation_metrics, openlineage_event, ...}
```

### 3.1 Pontos de saída e atomicidade

- O `try/except/finally` garante que **runs**, **state**, **quality**, **explain** e **lineage** são sempre persistidos, mesmo em falha. A única exceção é falha catastrófica antes da criação das ctrl tables ou em chamadas de log que ergam exceção (capturadas e logadas via `logger.error`).
- A escrita do **target** é **uma única operação Delta** (uma transação). Os logs em ctrl tables não fazem parte da mesma transação — são atomicidade independente, daí o `try/except` ao redor de cada bloco final.
- `delta_version_before/after` são lidos antes e depois para confirmar `write_committed`. Útil quando o motor não escreve nada (ex.: hash-diff sem mudanças) — `rows_written = 0` e versão não muda.

### 3.2 Variáveis "vivas" no escopo do `ingest_plan`

Estado mutável durante a execução, com defaults seguros para garantir que o `finally` sempre tenha valores consistentes:

| Variável                                        | Default            | Atualizado em           |
| ----------------------------------------------- | ------------------ | ----------------------- |
| `run_id`                                        | UUID novo          | início                  |
| `source_name`                                   | `"unknown"`        | passo [5]               |
| `wm_prev`, `wm_current`, `wm_candidate`         | `None`             | passos [6], [11], [25]  |
| `rows_read`, `rows_written`, `rows_quarantined` | `0`                | passos [10], [13], [20] |
| `status`                                        | `"SUCCESS"`        | exception → `"FAILED"`  |
| `quality_status`                                | `"NOT_CONFIGURED"` | passo [13]              |
| `schema_changes`, `operation_metrics`           | `{}`               | passos [9], [26]        |
| `delta_version_before/after`                    | `None`             | passos [18], [22]       |
| `write_committed`                               | `False`            | passo [23]              |
| `prepared_df`                                   | `None`             | passo [7]               |
| `row_metrics`                                   | zeros              | passos [27], exception  |
| `error`                                         | `None`             | exception               |

---

## 4. Submódulos em detalhe

### 4.1 `_spark.py` — Resolução de SparkSession

**Responsabilidade.** Resolver a `SparkSession` ativa de forma lazy, sem assumir Databricks Runtime.

**Estratégia de resolução** (`get_spark()`):

1. Tenta `from databricks.sdk.runtime import spark`. Em DBR isso devolve a sessão do cluster.
2. Se falhar (não está em DBR), tenta `SparkSession.getActiveSession()`.
3. Como fallback, lê `SparkSession._instantiatedSession`.
4. Se nenhuma sessão existir, ergue `RuntimeError` com mensagem clara em PT-BR.

**Proxy preguiçoso.** O atributo `spark` do módulo é uma instância de `_SparkProxy` cuja `__getattr__` resolve a sessão **na hora da chamada**. Isso permite que outros módulos façam `from ._spark import spark; spark.sql(...)` sem que a importação do pacote dispare a criação de sessão. Resultado: o pacote é importável em ambientes sem Spark configurado (útil em tooling, geração de docs, validação de plano em testes puros).

**Detecção de serverless** (`detect_serverless()`):

- Lê três configurações Spark e considera serverless se qualquer uma indicar.
- Resultado é cacheado em `_IS_SERVERLESS` (módulo-level) para evitar custo repetido.
- Falhas de leitura → `False` (assume cluster tradicional, comportamento mais conservador).

**`safe_cache(df, enabled)`**: tenta `df.cache()`; se rodar em serverless ou se Spark erguer `NOT_SUPPORTED`/`SERVERLESS`, devolve o DataFrame original sem cachear. Outros erros são propagados — não silenciamos falhas inesperadas.

**`safe_unpersist(df, enabled)`**: simétrico. Chamado no `finally` do orquestrador para liberar memória.

### 4.2 `_sql.py` — Helpers de SQL

Helpers puros (sem Spark), todos com escape correto:

| Função                                           | Propósito                                                                               |
| ------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `q(identifier)`                                  | Quota um identificador com crases, escapando crases internas. `users` → ``\`users\```   |
| `qt(table_name)`                                 | Quota nome com pontos preservando partes. `cat.sch.tbl` → ``\`cat\`.\`sch\`.\`tbl\```   |
| `full_table_name(c, s, t)`                       | Concatena `c.s.t` como string                                                           |
| `sql_lit(value)`                                 | Literal SQL: `None`→`NULL`, `bool`→`true/false`, outros viram `'...'` com `'` duplicado |
| `sql_int(v)`                                     | `None`→`NULL`, demais convertem com `int(v)`                                            |
| `to_json(v)`                                     | `json.dumps(v, default=str, ensure_ascii=False)` com fallback para `str(v)`             |
| `as_list(v, sep="\|")`                           | Aceita `None`/lista/iter/string com separador; remove vazios e faz strip                |
| `validate_cols(df, cols, ctx)`                   | Erra com `ValueError` se faltarem colunas                                               |
| `utc_now_ts()` / `utc_now_str()` / `today_str()` | Tudo em UTC; formato `%Y-%m-%d %H:%M:%S`                                                |
| `new_run_id()`                                   | UUID4 string                                                                            |
| `safe_truncate(text, max_len)`                   | Trunca em `max_len` (default `CONFIG.max_error_len = 8000`) e adiciona marcador         |

**Por que helpers e não query builder?** Aqui as queries são pontuais, conhecidas em compile-time, sem composição dinâmica perigosa. Helpers minimalistas são mais legíveis e auditáveis que SQLAlchemy/PySQL.

**Segurança contra SQL injection.** Identificadores externos sempre passam por `q`/`qt`. Literais sempre passam por `sql_lit`/`sql_int`. As únicas strings concatenadas direto são valores conhecidos (ex.: nomes de colunas internas, partições já validadas via `validate_cols`).

### 4.3 `config.py` — Configuração e tipos

**Tipos exportados** (todos `Literal` para narrowing estático):

```python
Layer = Literal["bronze", "silver", "gold"]
WriteMode = Literal["scd0_append", "scd0_overwrite", "scd1_upsert",
                    "scd1_hash_diff", "scd2_historical", "snapshot_soft_delete"]
MergeStrategy = Literal["delta", "delta_by_partition", "replace_partitions"]
SchemaPolicy = Literal["permissive", "additive_only", "strict"]
QualityFailAction = Literal["fail", "warn", "quarantine"]
Source = Union[str, DataFrame]
```

**`VALID_WRITE_MODES`**: set usado pela validação runtime (Literal só faz tipagem estática).

**`CONTROL_COLUMNS`**: conjunto de colunas que o framework adiciona/manipula. Importante: o cálculo de hash em `schema.py` exclui essas colunas para que mudanças neles não invalidem o `row_hash`.

```python
{"ingestion_date", "source_system", "__run_id", "row_hash",
 "valid_from", "valid_to", "is_current", "is_active", "deleted_at",
 "changed_columns"}
```

**`FrameworkConfig`** (frozen dataclass): defaults globais. A instância singleton `CONFIG` é importada por todos os módulos que precisam de defaults. Mudar defaults no projeto: monkey-patch ou (preferível) passar valores no `IngestionPlan` quando suportado.

Campos relevantes:

- `default_catalog="main"`, `default_source_system="default"`, `default_partition_col="ingestion_date"`
- `ctrl_schema="ops"`: schema onde as ctrl tables vivem.
- `ctrl_table_*`: nomes das ctrl tables (todas começam com `ctrl_ingestion_`).
- `max_error_len=8000`: trunca tracebacks para caber em colunas STRING.
- `default_lock_ttl_minutes=120`, `default_retry_attempts=3`, `default_retry_backoff_seconds=5`.
- `max_inline_accepted_values=1000`: protege contra listas de `accepted_values` gigantes — usuário deve usar tabela de referência + join.
- `max_partition_predicate_values=1000`: limite de valores distintos em `IN (...)` para SCD1/2 e snapshot.

### 4.4 `plan.py` — Contrato declarativo

#### 4.4.1 `QualityRules`

```python
@dataclass(frozen=True)
class QualityRules:
    required_columns: List[str] = field(default_factory=list)
    not_null: List[str] = field(default_factory=list)
    unique_key: List[str] = field(default_factory=list)
    accepted_values: Dict[str, List[Any]] = field(default_factory=dict)
    min_rows: Optional[int] = None
    max_null_ratio: Dict[str, float] = field(default_factory=dict)
    expressions: List[QualityExpression] = field(default_factory=list)
```

Frozen para passagem segura entre threads/jobs. Construtores aceitam dict via `normalize_quality_rules`.

#### 4.4.2 `IngestionPlan`

Frozen dataclass com 40+ campos. Agrupados por finalidade:

**Identificação** — `source` (str, DataFrame, `SourceSpec` ou `ConnectorSpec`), `target_table`, `catalog`, `layer`, `target_schema`, `mode`, `source_system`, `ctrl_schema`, `notebook_name`.

`layer` é a camada lógica Medallion usada por presets, restrições e observabilidade. `target_schema` é o schema físico do target; quando omitido, o framework usa `layer` para manter o padrão `{catalog}.{layer}.{target_table}`.

**Metadados de contrato** — `description`, `owner`, `domain`, `tags`, `sla`, `runtime_parameters`. Não mudam a escrita; são propagados para retorno e `ctrl_ingestion_runs`.

**Transformações** — `select_columns`, `filter_expression`, `custom_keys`.

**Chaves e dedup** — `merge_keys`, `hash_keys`, `hash_exclude_columns`, `dedup_order_expr`.

**Watermarks** — `watermark_columns` (lista; suporta composto).

**Particionamento e clustering** — `partition_column`, `partition_value`, `merge_strategy`, `merge_partition_column`, `cluster_columns`, `zorder_columns`, `optimize_after_write`.

**Schema** — `schema_policy`, `allow_type_widening`.

**Quality** — `quality_rules`, `on_quality_fail`.

**SCD2** — `scd2_change_columns`, `scd2_effective_from_column`.

**Encoding** — `fix_encoding`, `encoding`, `encoding_columns`.

**Diagnóstico** — `dry_run`, `explain_mode`, `explain_format`.

**Lineage** — `openlineage_enabled`, `openlineage_namespace`, `openlineage_producer`.

**Performance/Concorrência** — `use_cache`, `lock_enabled`.

**Linhagem operacional** (parent/master) — `parent_run_id`, `run_group_id`, `master_job_id`, `master_run_id`. Esses campos não influenciam o comportamento técnico; apenas são propagados nas ctrl tables para join em dashboards e correlação com orquestradores externos.

#### 4.4.3 `validate_write_mode(mode)`

Aceita `None`/string vazia → `"scd0_append"`. Caso contrário valida contra `VALID_WRITE_MODES`. Erra com lista ordenada dos válidos.

#### 4.4.4 `normalize_quality_rules(value)`

`None` → `None`. `QualityRules` → passthrough. `dict` → `QualityRules(**value)`. Outros tipos quebrarão no `**`.

#### 4.4.5 `build_plan_from_kwargs(**kwargs) -> IngestionPlan`

Função ponte entre a API procedural (`ingest(...)`) e o contrato `IngestionPlan`.

Pontos importantes:

- **Lista de parâmetros conhecida** (`_KNOWN_PARAMS`): qualquer kwarg fora dela ergue `ValueError("Parâmetros não reconhecidos em ingest(): [...]")`. Isso evita erros silenciosos por typos (ex.: `merg_keys=` em vez de `merge_keys=`).
- **Listas via `as_list`**: aceita string com `|`, lista, iterável, ou `None`. Padrão de usabilidade vindo do uso em notebooks Databricks (parâmetros vêm como string).
- **Custom keys**: dict de `nome_da_chave -> lista_de_colunas`; cada lista também passa por `as_list`.
- **Enums validados estritamente** via `_validate_enum` contra `VALID_LAYERS`, `VALID_MERGE_STRATEGIES`, `VALID_SCHEMA_POLICIES`, `VALID_QUALITY_FAIL_ACTIONS`, `VALID_EXPLAIN_FORMATS`. Typos em `layer`, `merge_strategy`, `schema_policy`, `on_quality_fail` ou `explain_format` viram `ValueError` com a lista de valores aceitos. `mode` continua passando por `validate_write_mode`.
- **`quality_rules`** passa por `normalize_quality_rules`, então aceita dict.
- **`column_mapping`** renomeia colunas source -> target antes da validação do plano; colisões, destinos duplicados e nomes técnicos reservados são bloqueados.
- **`delta_properties`** aplica TBLPROPERTIES na criação da tabela Delta.
- **`retry_attempts`/`retry_backoff_seconds`** sobrescrevem retry global por plano.
- **`SourceSpec`** aceita Autoloader declarativo legado com `trigger="available_now"`, `schema_location` e `checkpoint_location` obrigatórios.
- **`ConnectorSpec`** é o modelo unificado para sources declarativos: catálogo/SQL, arquivos, object storage/blob, JDBC, REST API e Autoloader.
- **`preset`/`presets`** são expandidos antes da normalização. O contrato explícito vence defaults e o plano guarda `applied_presets`.

### 4.5 `presets.py` — Defaults declarativos acopláveis

O módulo `presets.py` implementa uma camada de defaults para padrões comuns de ingestão, sem criar um segundo validador. O fluxo é:

1. `build_plan_from_kwargs()` chama `apply_preset(dict(kwargs))`.
2. `apply_preset()` aplica um ou mais presets em ordem.
3. O contrato explícito sobrescreve os defaults.
4. `build_plan_from_kwargs()` normaliza e valida o contrato expandido como qualquer outro contrato.

Funções públicas:

- `list_presets()`: lista presets registrados.
- `get_preset(name)`: devolve cópia defensiva do preset.
- `preset_details(name)`: retorna metadata para CLI/docs.
- `register_preset(name, preset, override=False)`: acopla presets internos sem editar o core.
- `apply_preset(contract)`: expande `preset`/`presets` para um contrato final.

Regras de combinação:

- Um contrato aceita apenas um preset principal de `kind="ingestion"`.
- Um contrato aceita apenas um preset de `kind="runtime"`.
- Presets `kind="modifier"` podem ser combinados livremente.
- `required_fields` do preset são verificados antes da construção do plano para gerar erro objetivo.

O catálogo built-in traz 18 presets de ingestão alinhados aos modos da lib e modificadores para quality, Delta properties, runtime e governança.

### 4.6 `shape.py` — Transformações JSON/struct/array

`shape.py` normaliza estruturas complexas antes de filtros, watermark, quality e escrita. Ele é intencionalmente separado de `annotations`: shape altera colunas e cardinalidade; annotations descreve catálogo.

Recursos:

- `flatten`: expande structs recursivamente com separador configurável.
- `columns`: extrai paths aninhados para aliases top-level.
- `arrays`: trata arrays com `keep`, `to_json`, `size`, `first`, `explode` e `explode_outer`.
- Guardrail de Bronze: `explode`/`explode_outer` falha por padrão, exigindo `allow_cardinality_change_on_bronze=true`.
- Guardrail de produto cartesiano: múltiplos explodes irmãos exigem `allow_cartesian=true`.
- Arrays aninhados podem ser declarados fora de ordem; o motor resolve quando o alias pai fica disponível.

Exemplo:

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
    item.sku: item_sku
    discount.code: discount_code
  flatten:
    enabled: true
    include: [customer]
```

### 4.7 `schema.py` — Hash, dedup, encoding e schema policy

#### 4.5.1 Hash determinístico

```python
def hash_columns(df, exclude_cols=None):
    exclude = set(CONTROL_COLUMNS)
    if exclude_cols: exclude.update(exclude_cols)
    return sorted([c for c in df.columns if c not in exclude])

def hash_from_cols(cols):
    if not cols:
        return F.unhex(F.sha2(F.lit(""), 256))
    return F.unhex(F.sha2(
        F.concat_ws("", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
        256
    ))
```

**Por que assim:**

- **Ordenação alfabética** de colunas → estabilidade entre execuções (resistente a reordenação no DataFrame).
- **Exclusão de `CONTROL_COLUMNS`** → `ingestion_date`, `source_system`, `__run_id` mudam toda execução; manter no hash invalidaria todas as linhas.
- **`concat_ws("", ...)`** com **Unit Separator (ASCII 0x1f)** como delimitador → caractere fora do espaço utilizável de quase todos os textos reais, evita colisão `"a", "b"` vs `"a|b"`.
- **`coalesce(..., "")`** com **NUL byte** como sentinela → distingue `NULL` de string vazia.
- **`sha2(..., 256)` + `unhex`** → bytes determinísticos de 32 octetos. Armazenado como `BINARY` no Delta.

**Implicações:**

- Igualdade de hash significa igualdade de **todas** as colunas não-controle (na ordenação alfabética). Útil para SCD1 hash-diff e SCD2.
- Colunas adicionais (não em `hash_keys`/`merge_keys`) entram no hash. Isso é intencional: queremos detectar qualquer mudança no payload.
- Para excluir colunas voláteis (ex.: timestamp gerado em ingestion), passe `hash_exclude_columns`.

#### 4.5.2 Deduplicação

```python
def deduplicate_by_order(df, keys, order_expr):
    df.createOrReplaceTempView(source_view)
    return spark.sql("""
        SELECT original_cols
        FROM (
            SELECT original_cols,
                   ROW_NUMBER() OVER (PARTITION BY keys ORDER BY order_expr) AS __rn
            FROM source_view
        )
        WHERE __rn = 1
    """)
```

- Usa `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY order_expr)` via SQL temp view; mantém a primeira linha pela ordem declarada.
- `order_expr` aceita `"col DESC NULLS LAST, other ASC"` como SQL de `ORDER BY`. Isso evita incompatibilidade de `F.expr("... DESC NULLS LAST")` em Spark Connect/serverless.
- Erra se o `order_expr` for vazio/só vírgulas.

**Quando entra no fluxo:** se `dedup_order_expr` é informado E há `merge_keys` ou `hash_keys`, `_prepare_dataframe` chama isso após `apply_watermark`.

#### 4.5.3 Custom keys

```python
build_custom_keys(df, {"composite_key": ["a", "b"]})
# → adiciona coluna "composite_key" = concat_ws("|", coalesce(a, ""), coalesce(b, ""))
```

Útil quando a chave lógica é compósita mas a tabela de destino quer uma coluna única (para `merge_keys` simples ou indexação).

#### 4.5.4 `fix_encoding(df, enabled, encoding, columns)`

Decodifica strings que vieram com encoding errado (clássico "Windows-1252 lido como UTF-8"). Estratégia:

- Se `enabled=False`, passthrough.
- Para cada coluna string (ou apenas as listadas), faz `decode(cast(col, binary), encoding)`.

Nota: isso assume que os bytes originais foram mantidos pelo Spark. Se o Spark já decodificou para UTF-8 (recodificando bytes), o `decode` não recupera. É melhor consertar na origem (ler com a charset correto), e usar `fix_encoding` só em emergência.

#### 4.5.5 `table_exists(full_name)`

Tenta primeiro `spark.catalog.tableExists("cat.sch.tbl")` (via split em `.`). Em caso de erro (ex.: nome com pontos não-padrão), faz fallback para `DESCRIBE TABLE`. Retorna bool.

#### 4.5.6 Schema policy

```python
def validate_schema_policy(df, target, policy, allow_type_widening=False):
    if not table_exists(target):
        return {"status": "new_table", ...}
    target_df = spark.read.table(target)
    src = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    tgt = {f.name: f.dataType.simpleString() for f in target_df.schema.fields}
    added = sorted(c for c in src if c not in tgt)
    removed = sorted(c for c in tgt if c not in src and c not in CONTROL_COLUMNS)
    type_changes = sorted(..., key=lambda change: change["column"])
    if policy == "strict" and (added or removed or type_changes): raise
    if policy == "additive_only" and (removed or unsafe_type_changes): raise
    if policy == "permissive" and unsafe_type_changes: raise
    return {"status": "checked", "added_columns", "removed_columns", "type_changes", "allow_type_widening"}
```

Comparação por nome canonical (`simpleString()` do Spark). Comparação **agnóstica de ordem** — adicionar uma coluna no meio não causa "removed".

**`removed`** ignora `CONTROL_COLUMNS`: mesmo se a fonte não traz `ingestion_date`, isso não é uma "remoção" — é apenas que a coluna é gerenciada pelo framework.

**`sync_delta_schema(df, target, schema_changes, policy)`** aplica `ALTER TABLE ADD COLUMNS` se houver `added_columns` e a policy permitir (`permissive` ou `additive_only`). Quando `allow_type_widening=True` e a mudança é reconhecida como alargamento seguro, aplica `ALTER TABLE ALTER COLUMN TYPE`. Nunca remove colunas automaticamente.

### 4.6 `watermark.py` — Watermark tipado

#### 4.6.1 Encoding

Watermarks são serializados como JSON com `type` e `value`:

```json
{
  "updated_at": {"type": "timestamp", "value": "2024-01-15 12:30:00"},
  "version": {"type": "bigint", "value": "42"}
}
```

- `type` vem do `simpleString()` da coluna no DataFrame, capturado em `_data_type_map(df)`.
- `value` é sempre `str(...)` ou `None`. JSON não preserva tipo Python, então a string + tipo é o que importa.
- `sort_keys=True` no `json.dumps` → estabilidade textual (útil em diffs e logs).

#### 4.6.2 Decoding e aplicação

`decode_watermark(raw, cols)` valida que todas as colunas requisitadas estão presentes e que cada item tem `value`. Erra com `ValueError` em payload inválido.

`_watermark_literal(df, col, values)` reconstrói o literal Spark com `cast(value, type)` — usando o tipo da **coluna do DataFrame atual**, não o tipo persistido. Isso é proposital: se o tipo da fonte mudou (ex.: era `int`, virou `bigint`), a comparação ainda funciona.

`apply_watermark(df, cols, last)`:

- Se `cols` ou `last` vazios → passthrough.
- **Watermark simples** (1 coluna): `df.where(col > literal)`.
- **Watermark composto** (n colunas): condição lexicográfica:

```
(c1 > L1)
 OR (c1 == L1 AND c2 > L2)
 OR (c1 == L1 AND c2 == L2 AND c3 > L3)
 ...
```

Isso é construído por `expr = F.lit(False); for i, c in cols: expr |= (eq_previous & col[i] > L[i])`.

#### 4.6.3 Persistência e leitura

`get_watermark(state_table, target_table, cols)` resolve o watermark anterior:

1. Lê `ctrl_ingestion_state` por `target_table` → coluna `watermark_value`.
2. Se vazio (estado nunca registrado), tenta computar `MAX()` direto da tabela de destino (resiliente a perda da state table).
3. Retorna `None` se ambos falharem ou tabela não existir.

`compute_watermark(df, cols)` calcula o **novo** watermark a partir do DataFrame. Para watermark composto usa `agg(F.max(F.struct(*cols)))` — Spark compara struct lexicograficamente.

#### 4.6.4 Edge cases

- DataFrame vazio → `compute_watermark` retorna `None`. O orquestrador, nesse caso, mantém `wm_prev` como current (não regredir).
- Coluna existe no DataFrame mas todos os valores são `NULL` → `MAX(col)` é `NULL` → retorna `None`.
- Watermark JSON corrompido → `ValueError` propaga; a execução falha (intencional).

### 4.7 `quality.py` — Quality gates e quarentena

#### 4.7.1 Por que single-pass

A versão original avaliava cada regra com seu próprio `df.where(...).count()`, multiplicando passagens sobre o DataFrame em datasets grandes. A versão atual consolida em uma única `df.agg(*aggs).collect()[0]`:

```python
agg_exprs = [F.count(F.lit(1)).alias("__total_rows")]
for c in null_cols_needed:
    agg_exprs.append(F.sum(F.col(c).isNull().cast("long")).alias(f"nulls__{safe(c)}"))
for c, values in rules.accepted_values.items():
    agg_exprs.append(F.sum(((~F.col(c).isin(values)) & F.col(c).isNotNull()).cast("long")).alias(...))
agg_row = df.agg(*agg_exprs).collect()[0]
```

Resultado: **uma passagem** sobre o DataFrame para todas as regras de coluna (`not_null`, `accepted_values`, `max_null_ratio`).

**Exceções** (passagens próprias):

- `unique_key` — exige `groupBy(...).count().where(count>1).count()`.
- `required_columns` — checagem só de schema, não toca dados.
- `quarantined_df` — `df.where(quarantine_condition)` é DataFrame lazy; `quarantined_df.count()` força ação adicional **apenas** se houve falha.

#### 4.7.2 Construção do `quarantine_condition`

A condição é construída acumulando OR a cada regra que falha:

```python
quarantine_condition = F.lit(False)
for c in not_null_failed: quarantine_condition |= F.col(c).isNull()
for c, values in accepted_values_failed: quarantine_condition |= ~F.col(c).isin(values) & F.col(c).isNotNull()
for c in max_null_ratio_failed: quarantine_condition |= F.col(c).isNull()
```

Apenas **regras que falham** entram na condição, evitando custo desnecessário quando tudo passa.

`unique_key`, `required_columns` e `min_rows` **não entram** no quarantine_condition — descrevem propriedades do conjunto, não das linhas isoladas:

- `unique_key`: qual linha "fica" e qual "vai"? Sem reprocessamento, decisão arbitrária.
- `required_columns`: a coluna inteira está faltando, não há linha a isolar.
- `min_rows`: contagem mínima é critério agregado.

Por isso o framework as classifica como **abort-only** (`quality.ABORT_ONLY_RULES`). Quando `on_quality_fail="quarantine"` e qualquer dessas regras falha, o orquestrador escala automaticamente para `"fail"` e aborta a execução com `ValueError`. A intenção é evitar o pior caso: hoje quarentena vazia (porque não havia condição de linha) levaria à escrita do dataset inteiro mesmo com `status=FAILED`.

#### 4.7.3 Detalhamento por regra

| Regra              | Avaliação                      | Falha quando         | Quarentena                     |
| ------------------ | ------------------------------ | -------------------- | ------------------------------ |
| `required_columns` | schema                         | coluna ausente do DF | n/a                            |
| `not_null`         | `sum(col.isNull())`            | count > 0            | linhas com NULL                |
| `unique_key`       | `groupBy(keys).count() > 1`    | algum grupo > 1      | não                            |
| `accepted_values`  | `sum(~isin(vals) & isNotNull)` | count > 0            | linhas inválidas (mantém NULL) |
| `min_rows`         | `count(*)`                     | total < min          | não                            |
| `max_null_ratio`   | `sum(col.isNull()) / total`    | razão > max          | linhas com NULL                |

#### 4.7.4 `accepted_values` com listas grandes

```python
if len(rules.accepted_values[c]) > CONFIG.max_inline_accepted_values:
    raise ValueError(f"... Use uma tabela de referência ...")
```

Default 1000. Acima disso, `isin([...])` no Spark vira problema (parser, plan grande, push-down ruim). Solução do framework: **rejeita a configuração** e orienta o usuário a usar uma tabela de referência + `LEFT ANTI JOIN`.

#### 4.7.5 Status final

- Sem regras → `status = "NOT_CONFIGURED"`. Não escreve em `ctrl_ingestion_quality`.
- Sem falhas → `status = "PASSED"`.
- Pelo menos uma falha → `status = "FAILED"`. A ação é decidida pelo orquestrador via `on_quality_fail`.

#### 4.7.6 Quarentena (`write_quarantine`)

Cada linha quarentenada vira:

```
run_id, target_table, rule_name, error_reason, record_payload, quarantined_at_utc
```

`record_payload` é `to_json(struct(*all_columns))` — serializa a linha inteira em JSON. Isso:

- Preserva todos os dados originais para auditoria.
- Independe do schema do target (que pode evoluir).
- Pode ser muito grande em linhas com muitas colunas. Não há corte automático.

`rule_name = "quality_gate"` (genérico) e `error_reason = json(failed_rules)` — não isolamos por regra individual; a auditoria recupera o motivo via `error_reason` ou cruzando com `ctrl_ingestion_quality`.

### 4.8 `state.py` — Tabelas de controle, log, lock, retry

#### 4.8.1 `ensure_ctrl_tables(catalog, schema)`

Idempotente: cria o schema (`CREATE SCHEMA IF NOT EXISTS`) e cada ctrl table (`CREATE TABLE IF NOT EXISTS`). Retorna dict `{logical_name -> full_qualified_name}`.

Schemas detalhados estão em [§9](#9-tabelas-de-controle--esquemas-e-papéis).

**Importante:** o framework aplica apenas migrações aditivas conhecidas (`ALTER TABLE ADD COLUMNS`). Nunca remove ou renomeia colunas automaticamente.

#### 4.8.2 `log_run(tables, payload)`

`INSERT` direto em `ctrl_ingestion_runs` com tipos coercidos:

- Inteiros → `sql_int` (NULL ou número literal).
- Booleanos → `"true"`/`"false"`.
- `duration_seconds` → `float`.
- `run_date` → `CAST('...' AS DATE)`.
- Timestamps → `CAST('...' AS TIMESTAMP)`.
- Strings (incluindo JSON) → `sql_lit` com escape de `'`.
- `error_message` → mensagem curta; traceback completo vai para `ctrl_ingestion_errors.stack_trace`.

#### 4.8.3 `upsert_state(...)`

`MERGE` em `ctrl_ingestion_state` por `target_table`. Atualiza watermark, status, run_id, métricas, error, e linhagem operacional (parent/master).

Existe **uma linha por target_table** — sempre o estado mais recente. Histórico fica em `ctrl_ingestion_runs`.

#### 4.8.4 `acquire_lock(...)`

```sql
MERGE INTO ctrl_ingestion_locks
USING (SELECT target, run_id, owner, NOW(), NOW()+INTERVAL X MINUTES, ttl, NULL, 'ACTIVE') s
ON t.target_table = s.target_table
WHEN MATCHED AND (status <> 'ACTIVE' OR expires < NOW()) THEN UPDATE *
WHEN NOT MATCHED THEN INSERT *
```

Depois faz `read.table(...).where(target).first()` para confirmar que o `run_id` ficou. Se outro `run_id` venceu, ergue `RuntimeError` com `owner`, `acquired_at_utc`, `expires_at_utc` e `ttl_minutes`.

**Best-effort.** Há uma janela de corrida: `MERGE` e o read-back não são atômicos do ponto de vista da linha. Documentado no docstring. Para correção forte, confiar no Delta optimistic concurrency control + retry.

`release_lock(...)` apenas faz UPDATE marcando `RELEASED`; nunca ergue (loga warning em falha).

#### 4.8.5 `with_retry(fn, attempts, backoff)`

Retry **somente para erros de concorrência Delta**:

```python
text = str(exc).upper()
retryable = any(t in text for t in ["CONCURRENT", "CONFLICT", "RETRY", "DELTA_CONCURRENT"])
if not retryable or attempt == attempts: raise
sleep_seconds = backoff_seconds * attempt + random.random()
```

Backoff linear + jitter. Não é exponencial — para o caso de uso (escritas Delta com janelas de partição estreitas) costuma ser suficiente.

**Não é genérico.** Não retenta `OutOfMemoryError`, falhas de conexão, schema mismatch, etc. — esses devem falhar rápido.

### 4.9 `writers.py` — Motores de escrita

Cada modo tem sua função. Todas seguem um padrão:

1. Validar colunas obrigatórias.
2. `ensure_delta_table` se ainda não existe.
3. Calcular `count` (do efetivo a escrever).
4. Se 0, retornar 0 sem escrever.
5. Escrever (append/overwrite/merge).
6. Retornar contagem.

#### 4.9.1 `ensure_delta_table(df, target, cluster_cols, partition_col)`

Se a tabela existe → `False`, no-op.

Se não existe → cria com schema vazio (`df.limit(0)`) usando `mergeSchema=true`. Aplica:

- `partitionBy(partition_col)` se há partição e não há cluster.
- `CLUSTER BY (cluster_cols)` se há cluster (Delta Liquid Clustering, mutuamente exclusivo com partição neste código).

**Não escreve dados aqui** — só cria a estrutura. O motor então faz a escrita real.

#### 4.9.2 `delta_version(target)` e `latest_operation_metrics(target)`

Lêem `DESCRIBE HISTORY ... LIMIT 1`. Cada chamada é uma query Spark (não é cacheado). O orquestrador chama estrategicamente: uma vez antes (`delta_version_before`), uma vez depois (`delta_version_after`), e mais uma para `operationMetrics`.

#### 4.9.3 `extract_row_metrics(metrics)` e `resolve_write_metrics(...)`

Mapeia `operationMetrics` do Delta:

```python
{
    "rows_inserted": parse("numTargetRowsInserted", "numOutputRows"),
    "rows_updated":  parse("numTargetRowsUpdated"),
    "rows_deleted":  parse("numTargetRowsDeleted"),
}
```

**Heurística:** para MERGE, Delta retorna os três `numTargetRows*`. Para APPEND/WRITE, só `numOutputRows`. Caímos para `numOutputRows` em `rows_inserted` quando o primeiro nome falta — isso vale para `scd0_append` e `scd1_hash_diff`.

`resolve_write_metrics` sempre adiciona `operation_metrics.logicalMetrics` com os contadores calculados pela biblioteca. Quando o Delta history traz `operationMetrics`, `metrics_source="mixed"`; caso contrário, `metrics_source="logical"`.

Limitação: para `scd0_overwrite` o mapping fica enganoso (overwrite tecnicamente "deleta tudo e insere"). O Delta retorna `numOutputRows` mas as linhas anteriores não aparecem em `numTargetRowsDeleted` (essa métrica só existe em DELETE/MERGE). Tratamos como insert simples — auditoria fina deve consultar `DESCRIBE HISTORY`.

#### 4.9.4 `affected_partition_values(df, partition_col)`

Coleta valores distintos da coluna de partição até `CONFIG.max_partition_predicate_values` (default 1000). Loga warning se atingir o limite — sinaliza que o predicado pode estar truncado.

Usado em:

- SCD1 hash diff: pré-filtra `target_df` por partições da source.
- SCD1 upsert com `delta_by_partition`: adiciona predicado na cláusula MERGE.
- `replace_partitions`: forma o `replaceWhere`.

#### 4.9.5 `write_append`

Mais simples. `df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)`. `mergeSchema=true` é seguro porque já foi validado por `validate_schema_policy`.

#### 4.9.6 `write_overwrite`

Suporta `replaceWhere` quando há `partition_col` + `partition_value` (e não há cluster). Sem isso, é overwrite total da tabela.

```python
.option("replaceWhere", f"`{partition_col}` = '{escape(partition_value)}'")
```

A escapagem é via duplicação de `'` (SQL standard).

#### 4.9.7 `write_upsert` (SCD1)

Três estratégias (`merge_strategy`):

**`delta`** (default). MERGE puro com `t.k <=> s.k` (operador IS NOT DISTINCT FROM, trata `NULL = NULL` como verdade — útil para chaves nulificáveis).

**`delta_by_partition`**. Mesma coisa, mas adiciona `AND t.partition_col IN (vals)` na cláusula ON. Reduz arquivos varridos quando o source só toca poucas partições.

**`replace_partitions`**. Não faz MERGE; faz `OVERWRITE` com `replaceWhere = partition_col IN (vals)`. **Mais rápido** quando o source contém o estado completo das partições afetadas (ex.: refeitura diária por `dt`). Cuidado: linhas que existiam no target mas não no source serão perdidas — por isso o plano exige `merge_partition_column` e `replace_partitions_source_complete=True`. Se `partition_column` também for informado, ele deve ser igual a `merge_partition_column`.

A view temporária `__ingest_src_<uuid>` é criada e descartada num `try/finally`.

#### 4.9.8 `write_scd1_hash_diff`

Append-only, mas **só de mudanças**:

1. `df_hashed = add_row_hash(df, hash_exclude)` — adiciona `row_hash`.
2. Ensure table; se vazia, append direto.
3. Lê target, opcionalmente filtra por partições afetadas.
4. **Defasagem do "atual":** `latest_order_expr` define o que é o "registro mais recente" do target. Sem expressão explícita, usa `ingestion_sequence` ou `ingestion_ts_utc`; target legado com múltiplas versões por chave e sem ordenação determinística falha com mensagem objetiva.
5. `target_latest = deduplicate_by_order(target_df, hash_keys, order_expr)`.
6. Anti-join lógico via `LEFT JOIN` + `WHERE __tgt_row_hash IS NULL OR s.row_hash != __tgt_row_hash`.
7. Append do diff.

**Edge case:** target legado sem `ingestion_ts_utc`/`ingestion_sequence` e com múltiplas versões por chave não tem "último estado" confiável. O framework rejeita e orienta informar `dedup_order_expr` ou migrar o target com uma coluna técnica determinística.

#### 4.9.9 `write_snapshot_soft_delete`

Snapshot completo: source representa o estado-fim. Linhas ausentes ficam marcadas `is_active=false` + `deleted_at=now()`.

Decisão arquitetural: ver [ADR-003](./adrs/ADR-003-snapshot-soft-delete-sql-merge.md).

Implementação via `MERGE` SQL em todos os runtimes, evitando divergência entre cluster classic e Databricks Serverless/Spark Connect:

```python
MERGE INTO target t
USING source_view s
ON t.key <=> s.key
WHEN MATCHED AND (NOT (t.row_hash <=> s.row_hash) OR t.is_active = false)
  THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...)
WHEN NOT MATCHED BY SOURCE AND t.is_active = true THEN UPDATE SET
  t.is_active = false,
  t.deleted_at = current_timestamp()
```

`whenNotMatchedBySource` é o que diferencia SCD1 de snapshot: linhas presentes no target **e ausentes** no source são marcadas inativas.

`is_active=false → set` na cláusula matched garante "ressuscitar" um registro que voltou a aparecer (deletado e reinserido).

**Bloqueio por design:** o orquestrador **rejeita** com `ValueError` a combinação `mode=snapshot_soft_delete` + `watermark_columns` ou `filter_expression`. Quando `source` é `ConnectorSpec`, o contrato precisa declarar `source.read.source_complete=true` ou `source.read.full_snapshot=true`. Snapshot parcial faria com que todas as linhas fora do filtro virassem inativas erroneamente. Para sincronização incremental, use `scd1_upsert`, `scd1_hash_diff` ou outro modo incremental.

#### 4.9.10 `write_scd2`

O modo mais complexo. Histórico completo: cada mudança gera nova linha, antiga fica marcada `is_current=false` com `valid_to`.

**Hash em SCD2 usa `change_cols` (não todas as colunas)**: `row_hash = hash_from_cols(change_cols)`. Mudanças em colunas fora de `change_cols` **não** geram nova versão. Útil para ignorar campos voláteis sem semântica de negócio.

Se `change_cols` não for informado, usa todas exceto `keys` e `CONTROL_COLUMNS`.

**Algoritmo:**

1. `src` enriquecido com `valid_from`, `valid_to=NULL`, `is_current=true`, `row_hash`, `changed_columns=NULL`.
2. Se target vazio, append direto e termina.
3. `target_current = target.where(is_current=true).select(keys, row_hash AS __tgt_row_hash)`.
4. `changed = src LEFT JOIN target_current ON keys WHERE __tgt_row_hash IS NULL OR src.row_hash != __tgt_row_hash`.
5. **Truque do staging para forçar INSERT em chaves reaparecidas:** linhas precisam tanto fechar a versão antiga (UPDATE) quanto inserir a nova (INSERT). Em MERGE Delta, uma mesma linha source pode disparar UPDATE OU INSERT, não ambos. Solução: replica cada linha que tinha match para ter `__merge_key_*` igual à chave (UPDATE), e mantém com `__merge_key_*=NULL` (INSERT). Para chaves que **não** tinham match (novas) só vai a versão com `__merge_key_*=NULL` (INSERT puro).

```
insert_stage  = changed                       (todos, __merge_key_*=NULL)
update_stage  = changed.where(__tgt_hash IS NOT NULL) (só matches, __merge_key_*=k)
staged        = insert_stage UNION update_stage
```

6. MERGE com condição `t.k <=> s.__merge_key_k AND t.is_current=true`:
   
   - `WHEN MATCHED AND row_hash <> row_hash` → fecha antiga (`valid_to=now`, `is_current=false`, `changed_columns`).
   - `WHEN NOT MATCHED` → INSERT (das novas + das versões "atuais" das chaves reaparecidas).

7. `changed_columns` é construído por SQL CASE: para cada `change_col`, `CASE WHEN NOT(t.c <=> s.c) THEN 'c' ELSE NULL END`, depois `concat_ws(',', ...)` — string CSV das colunas mudadas.

#### 4.9.11 `execute_write_mode(plan, df, target, effective_rows)`

Dispatcher por `plan.mode`. Em SCD1, calcula `affected_partition_values` para `merge_partition_column` (ou `partition_column` como fallback), evitando trabalho desnecessário quando o modo não precisa.

### 4.10 `lineage.py` — Explain e OpenLineage

#### 4.10.1 `capture_explain(df, mode="formatted")`

Redireciona `stdout` durante `df.explain(mode)` para capturar o plano como string. Tenta o método novo (com kwarg `mode`); cai para `df.explain(True)` em versões antigas. Resultado é truncado em `100_000` chars antes de ser inserido em `ctrl_ingestion_explain`.

#### 4.10.2 OpenLineage

Eventos OpenLineage 1.0.5 com facets:

- **Run facet `parent`** — só presente se `parent_run_id` existir.
- **Run facet `processing_engine`** — engine=spark + version.
- **Job facet `sourceCodeLocation`** — type=notebook, url=notebook_name.
- **Input dataset facet `schema`** — colunas do `prepared_df`.
- **Output dataset facet `schema`** — colunas do target após escrita.
- **Output dataset facet `dataQualityMetrics`** — só `rowCount = rows_written` (não fazemos métricas detalhadas no facet padrão).
- **Run facet customizado `lakehouse_ingestion`** — modo, layer, rowsRead/Written, deltaVersionBefore/After, operationMetrics, started/finishedAt.

`_clean_none(value)` recursivamente remove chaves `None` antes da serialização — evita poluir o JSON com `null` em facets opcionais.

`namespace` default: `databricks://<catalog>`. Pode ser sobrescrito via `openlineage_namespace`.

`eventType` é `COMPLETE` em sucesso, `FAIL` em erro.

`event_json` é gravado como string em `ctrl_ingestion_lineage`. Para um collector OpenLineage real (Marquez, OpenLineage proxy), você precisa de um forwarder externo lendo essa tabela ou um wrapper que faça POST HTTP no `_write_openlineage_event`.

### 4.11 `ingestion.py` — Orquestrador

Mantém apenas:

- Imports e logger.
- `_resolve_source(plan)` — abre table, usa DataFrame ou resolve `ConnectorSpec` via registry. Quando `source` é `SourceSpec` ou `ConnectorSpec(connector="autoloader")`, `ingest_plan` despacha para `ingest_stream_plan`.

### Streaming `available_now`

`SourceSpec(type="autoloader")` ou `ConnectorSpec(connector="autoloader")` usa `spark.readStream.format("cloudFiles")` com `trigger(availableNow=True)`. O stream é finito: processa arquivos disponíveis no checkpoint e termina.

Cada micro-batch chama `ingest_plan` com `source=batch_df`, `parent_run_id=stream_run_id`, `lock_enabled=False` e `idempotency_key="<stream-key>:batch:<batch_id>"`. Isso preserva a semântica at-least-once do `foreachBatch` sem duplicar batches já concluídos.

O ciclo externo fica em `ctrl_ingestion_streams`; os runs filhos continuam em `ctrl_ingestion_runs`.
- `_prepare_dataframe(df, plan, run_id, run_date, wm_prev)` — encadeia transformações pré-quality.
- `_validate_plan(plan, df, target)` — regras de negócio + schema policy.
- `_build_dry_run_result(...)` — payload de retorno em dry-run.
- `_finalize_execution(...)` — chama `log_run` com payload completo.
- `ingest_plan(plan)` — `try/except/finally` central (ver §3).
- `ingest(**kwargs)` — wrapper que constrói o plan e delega.
- `EXAMPLE_BRONZE_PLAN`, `EXAMPLE_SILVER_PLAN` — dicts de exemplo.

Reduzido de ~1800 linhas (versão original monolítica) para ~430 linhas, mantendo 100% da lógica.

---

## 5. Modos de escrita — semântica e garantias

### 5.1 Tabela comparativa

| Modo                   | Estratégia                       | Idempotência             | Histórico                      | Required                                     | Aviso                                       |
| ---------------------- | -------------------------------- | ------------------------ | ------------------------------ | -------------------------------------------- | ------------------------------------------- |
| `scd0_append`          | APPEND                           | ❌ (acumula sempre)       | ❌                              | —                                            | Adicione watermark p/ idempotência relativa |
| `scd0_overwrite`       | OVERWRITE [+ replaceWhere]       | ✅ (sobrescreve)          | ❌                              | — (opcional partition_col + partition_value) | Sem `replaceWhere`, apaga tudo              |
| `scd1_upsert`          | MERGE                            | ✅ (estado atual)         | ❌                              | `merge_keys`                                 | Não disponível em bronze                    |
| `scd1_hash_diff`       | LEFT JOIN + APPEND diff          | ✅ relativa (hash)        | ❌ (sem versionar)              | `hash_keys`                                  | Default order assume `ingestion_date`       |
| `scd2_historical`      | MERGE+UNION (staging)            | ✅ (versões)              | ✅                              | `merge_keys`, opc. `scd2_change_columns`     | Não disponível em bronze                    |
| `snapshot_soft_delete` | MERGE WITH NOT MATCHED BY SOURCE | ✅ (estado + soft-delete) | ✅ via `is_active`/`deleted_at` | `merge_keys`, source completa                | Rejeita `watermark_columns`/`filter_expression` |

### 5.2 Restrições de camada

```python
if plan.layer == "bronze" and plan.mode in {"scd1_upsert", "scd2_historical", "snapshot_soft_delete"}:
    raise ValueError("Bronze deve ser orientada a captura...")
```

Bronze deve preservar a fonte sem reinterpretação. Modos transacionais ficam para silver/gold.

### 5.3 Combinações com watermark

| Modo                   | Watermark é coerente? | Comentário                                                        |
| ---------------------- | --------------------- | ----------------------------------------------------------------- |
| `scd0_append`          | ✅ ideal               | filtra novos eventos                                              |
| `scd0_overwrite`       | ⚠️ depende            | overwrite sobrescreve tudo; watermark afeta só o source           |
| `scd1_upsert`          | ✅                     | MERGE incremental; processa só novos                              |
| `scd1_hash_diff`       | ✅                     | reduz scan da source                                              |
| `scd2_historical`      | ⚠️                    | watermark precisa garantir "atualidade"; cuidado com out-of-order |
| `snapshot_soft_delete` | ❌                     | rejeitado com `ValueError`; snapshot exige source completo        |

### 5.4 Garantias transacionais

Cada motor de escrita executa **uma única transação Delta**. SCD2 e snapshot são MERGE (atômico). Append e overwrite são writes simples (atômicos). Hash-diff faz APPEND atômico após calcular o diff em memória.

O retry em `with_retry` cuida especificamente de `ConcurrentAppendException` e similares — comportamento padrão Delta.

---

## 6. Quality gates — avaliação consolidada

Ver §4.7 para o detalhamento. Resumo dos invariantes:

1. **Sem regras** → `NOT_CONFIGURED`. Nenhuma escrita em `ctrl_ingestion_quality`.
2. **Regras de coluna** (`not_null`, `accepted_values`, `max_null_ratio`) são avaliadas em **uma agregação consolidada**.
3. **Regras estruturais** (`required_columns`, `min_rows`, `unique_key`) ficam fora da agregação consolidada por exigirem semânticas próprias.
4. **`accepted_values` > 1000 valores** → erro de configuração.
5. **`quarantine`** isola apenas linhas atingidas por `not_null`/`accepted_values`/`max_null_ratio`. As demais regras (`unique_key`, `min_rows`, `required_columns`) são classificadas como **abort-only** em `quality.ABORT_ONLY_RULES`: relatam falha mas não conseguem isolar linhas. Quando `on_quality_fail="quarantine"` e qualquer dessas regras falha, o orquestrador escala automaticamente para `"fail"`.
6. Cada `failed_count` é uma contagem real (não amostragem). Em datasets grandes, isso ainda é uma passagem completa, mas única.

### 6.1 Ações de falha (`on_quality_fail`)

- `fail` (default) — `raise ValueError(json(failed_rules))`. Aborta `ingest_plan`. `status="FAILED"` no return e em `ctrl_ingestion_runs`. Watermark **não** avança (`upsert_state` é chamado com `wm_prev`).
- `quarantine` — escreve quarentena, `prepared_df = valid_df`, segue para escrita. `effective_rows = rows_read - rows_quarantined`. Watermark avança normalmente. **Escalado para `fail`** se a falha vier de uma regra abort-only (`unique_key`, `min_rows`, `required_columns`).
- `warn` — apenas loga warning. Toda a fonte é escrita, incluindo linhas problemáticas. Use só em modo de tolerância controlada.

### 6.2 Combinação com dry_run

`dry_run=True` é **sem efeitos colaterais**: não cria schemas/ctrl tables (`ctrl_table_names` é usado em vez de `ensure_ctrl_tables`), não aplica `sync_delta_schema` (`_validate_plan(..., apply_changes=False)`), não persiste `write_explain_plan`/`write_quality_results`/`write_quarantine`, não chama `upsert_state` nem `log_run`, não emite OpenLineage. O retorno (`status="DRY_RUN"`) inclui `quality_status`, `rows_quarantined`, `affected_partitions`, `watermark_*`, `schema_changes` — todas as validações rodam, só os escritos são suprimidos.

---

## 7. Schema policy — políticas e ALTER automático

### 7.1 Quando rodam

`_validate_plan` chama `validate_schema_policy(df, target, policy, allow_type_widening)` **depois** de `_prepare_dataframe` (DataFrame final, com colunas de controle adicionadas) e **antes** da escrita.

`sync_delta_schema(df, target, schema_changes, policy)` é chamado **na mesma etapa** se houver `added_columns` e a política permitir.

### 7.2 Comportamento por política

```
                permissive         additive_only       strict
                -----------------  -----------------  -------
new_table       OK + create         OK + create        OK + create
added cols      ALTER ADD           ALTER ADD          RAISE
removed cols    OK (no ALTER)       RAISE              RAISE
safe widening   ALTER TYPE*         ALTER TYPE*        RAISE
unsafe changes  RAISE               RAISE              RAISE
```

`*` exige `allow_type_widening=True`. A validação reconhece alargamentos simples (`tinyint -> int -> bigint`, `float -> double`, aumento de precisão/escala decimal e `date -> timestamp`). Mudanças inseguras falham antes da escrita.

`additive_only` é o default recomendado em silver/gold: protege contra perda de coluna ou mudança de tipo não-intencional, mas permite evolução por adição.

`strict` é para tabelas com contrato externo forte (consumidores BI, APIs).

### 7.3 Colunas adicionadas em ALTER

```python
fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
cols_sql = ", ".join(f"{q(c)} {fields[c]}" for c in added if c in fields)
spark.sql(f"ALTER TABLE {qt(target)} ADD COLUMNS ({cols_sql})")
```

Tipos vêm do DataFrame, não inferidos manualmente. As colunas adicionadas ficam **no fim** do schema (Delta não suporta posicionar). Aceitam NULL para registros antigos.

Mudanças detectadas são registradas em `ctrl_ingestion_schema_changes`; isso preserva auditoria mesmo quando a evolução é aplicada automaticamente.

---

## 8. Watermarks — encoding, aplicação e estado

Ver §4.6 para detalhes técnicos. Pontos operacionais:

### 8.1 Resiliência

`get_watermark` cai em três níveis:

1. `ctrl_ingestion_state.watermark_value` — fonte primária.
2. `MAX(col)` direto da target — backup (state perdida).
3. `None` — primeira execução ou tabela vazia.

Isso garante que uma queda da `ctrl_ingestion_state` não causa reprocessamento total. Mantém-se um "memory of last good" no próprio target.

### 8.2 Watermark candidate vs current

- **`wm_candidate`** — calculado **antes** da escrita, sobre o DataFrame preparado. Usado em logs e como contingência se `wm_current` falhar (ex.: erro depois do write).
- **`wm_current`** — calculado **depois** da escrita, mas hoje vem do mesmo `prepared_df`. A diferença operacional é a janela: se algo der errado entre o write e o cálculo, `wm_current` ainda é coerente porque o DataFrame não muda.

Em falha, `upsert_state` recebe `wm_prev` (não avança) — comportamento correto, evita pular janela.

### 8.3 Out-of-order

Watermark assume que dados chegam em ordem. Eventos atrasados (`event_time < last_watermark`) **não serão capturados** em execuções futuras se entrarem no mesmo source. Para late data, considere:

- `scd1_hash_diff` — captura mudanças mesmo sem watermark.
- Watermark composto (ex.: `event_time, ingestion_id`) com tolerância — mas o framework não tem watermark "com lateness window".

---

## 9. Tabelas de controle — esquemas e papéis

Todas vivem em `<catalog>.<ctrl_schema>` (default `<catalog>.ops`). Todas USING DELTA.

### 9.1 `ctrl_ingestion_runs`

**Particionada por `run_date`.** Uma linha por execução de `ingest_plan`. Histórica.

| Campo                                                                                            | Tipo              | Descrição                        |
| ------------------------------------------------------------------------------------------------ | ----------------- | -------------------------------- |
| `run_id`                                                                                         | STRING            | UUID4                            |
| `run_ts_utc`, `run_date`                                                                         | TIMESTAMP, DATE   | timestamp e dia da execução      |
| `notebook_name`                                                                                  | STRING            | `plan.notebook_name`             |
| `layer`, `source_table`, `target_table`, `mode`                                                  | STRING            | identificação                    |
| `status`                                                                                         | STRING            | `SUCCESS` / `FAILED` / `DRY_RUN` |
| `rows_read`, `rows_written`, `rows_inserted`, `rows_updated`, `rows_deleted`, `rows_quarantined` | BIGINT            | contagens                        |
| `watermark_column`, `watermark_previous`, `watermark_current`                                    | STRING            | watermark snapshot               |
| `started_at_utc`, `finished_at_utc`, `duration_seconds`                                          | TIMESTAMP, DOUBLE | janela                           |
| `quality_status`, `schema_policy`                                                                | STRING            | snapshot de configuração         |
| `schema_changes_json`, `stage_durations_json`, `operation_metrics_json`                          | STRING            | JSONs aninhados                  |
| `contract_description`, `contract_owner`, `contract_domain`, `contract_sla`                      | STRING            | metadados declarativos           |
| `contract_tags_json`, `runtime_parameters_json`                                                  | STRING            | metadados em JSON                |
| `write_started_at_utc`, `write_finished_at_utc`                                                  | TIMESTAMP         | só do passo de escrita           |
| `delta_version_before`, `delta_version_after`, `write_committed`                                 | BIGINT, BOOLEAN   | atomicidade                      |
| `error_message`                                                                                  | STRING            | mensagem curta do erro            |
| `parent_run_id`, `run_group_id`, `master_job_id`, `master_run_id`                                | STRING            | linhagem operacional             |
| `idempotency_key`, `idempotency_policy`, `skip_reason`, `skipped_by_run_id`                      | STRING            | idempotência e skips             |
| `framework_version`, `ctrl_schema_version`                                                       | STRING, BIGINT    | versão da lib e das ctrl tables  |
| `runtime_type`, `spark_version`, `python_version`                                                | STRING            | ambiente de execução             |

Padrões de consulta úteis:

```sql
-- runs com falha hoje
SELECT run_id, target_table, error_message
FROM ops.ctrl_ingestion_runs
WHERE run_date = current_date() AND status = 'FAILED';

-- duração média por target
SELECT target_table, avg(duration_seconds) FROM ops.ctrl_ingestion_runs
WHERE run_date >= current_date() - 7 GROUP BY target_table;
```

### 9.2 `ctrl_ingestion_errors`

Tabela particionada por `error_date` para diagnóstico detalhado de falhas. Uma linha por execução com erro.

| Coluna                                                                                           | Tipo              | Observação                         |
| ------------------------------------------------------------------------------------------------ | ----------------- | ---------------------------------- |
| `run_id`, `target_table`, `source_table`, `mode`, `status`                                       | STRING            | contexto operacional               |
| `error_ts_utc`, `error_date`                                                                     | TIMESTAMP, DATE   | momento e partição do erro         |
| `error_type`, `error_message`, `stack_trace`                                                     | STRING            | classe, mensagem curta e traceback |
| `framework_version`, `ctrl_schema_version`, `runtime_type`, `spark_version`, `python_version`    | STRING/BIGINT     | suporte e auditoria                |

### 9.3 `ctrl_ingestion_schema_changes`

Tabela de auditoria da evolução estrutural do destino.

| Coluna                                                                                           | Tipo              | Observação                         |
| ------------------------------------------------------------------------------------------------ | ----------------- | ---------------------------------- |
| `run_id`, `target_table`, `column_name`                                                          | STRING            | execução e coluna afetada          |
| `change_ts_utc`                                                                                  | TIMESTAMP         | momento do registro                |
| `change_type`                                                                                    | STRING            | `add_column` ou `type_change`      |
| `source_type`, `target_type`                                                                     | STRING            | tipo novo e tipo anterior          |
| `applied`                                                                                        | BOOLEAN           | se o ALTER foi aplicado            |
| `details_json`                                                                                   | STRING            | payload completo da validação      |
| `framework_version`, `ctrl_schema_version`                                                       | STRING/BIGINT     | suporte e auditoria                |

### 9.4 `ctrl_ingestion_state`

**Uma linha por target_table.** Snapshot do último estado conhecido. PK em `target_table`.

| Campo                                                                                          | Tipo                               |
| ---------------------------------------------------------------------------------------------- | ---------------------------------- |
| `target_table` (PK)                                                                            | STRING                             |
| `watermark_column`, `watermark_value`                                                          | STRING (JSON em `watermark_value`) |
| `last_success_at_utc`, `last_run_id`, `last_status`, `last_rows_written`, `last_error_message` | mistos                             |
| `parent_run_id`, `run_group_id`, `master_job_id`, `master_run_id`                              | STRING                             |
| `last_delta_version`, `last_write_completed_at_utc`                                            | BIGINT, TIMESTAMP                  |
| `last_watermark_candidate`, `last_updated_at_utc`                                              | STRING, TIMESTAMP                  |

Use para dashboards de "última execução" e detectar tabelas que não rodam há X dias.

### 9.5 `ctrl_ingestion_quality`

Uma linha **por regra falhada** por execução.

| Campo                                                     |
| --------------------------------------------------------- |
| `run_id`, `target_table`, `rule_name`, `status`           |
| `severity`, `message`                                     |
| `failed_count` (BIGINT), `checked_at_utc`, `details_json` |

`rule_name` formato: `not_null:<col>`, `accepted_values:<col>`, `max_null_ratio:<col>`, `unique_key`, `min_rows`, `required_columns`.

Não recebe linhas em `status=PASSED`.

### 9.6 `ctrl_ingestion_quarantine`

Uma linha **por linha quarentenada**, com payload JSON da linha original.

| Campo                                                 |
| ----------------------------------------------------- |
| `run_id`, `target_table`, `rule_name`, `error_reason` |
| `record_payload` (STRING JSON), `quarantined_at_utc`  |

Pode crescer rápido se `on_quality_fail=quarantine` for usado com fontes "sujas". Considere TTL/VACUUM.

### 9.7 `ctrl_ingestion_locks`

Uma linha **por target_table** (PK). Renovada a cada execução.

| Campo                                                                        |
| ---------------------------------------------------------------------------- |
| `target_table` (PK), `run_id`, `owner`, `acquired_at_utc`, `expires_at_utc`, `ttl_minutes`, `released_at_utc`, `status` |

`status` ∈ `{ACTIVE, RELEASED}`. Locks expirados são "rompidos" no próximo `acquire_lock`.

### 9.8 `ctrl_ingestion_explain`

Plano Spark capturado.

| Campo                                                                     |
| ------------------------------------------------------------------------- |
| `run_id`, `target_table`, `source_table`, `mode`                          |
| `explain_format`, `plan_text` (truncado em 100k chars), `captured_at_utc` |

Só recebe linhas se `explain_mode=True`.

### 9.9 `ctrl_ingestion_lineage`

Eventos OpenLineage como JSON.

| Campo                                                    |
| -------------------------------------------------------- |
| `run_id`, `event_time_utc`, `event_type` (COMPLETE/FAIL) |
| `target_table`, `source_table`, `namespace`, `producer`  |
| `event_json` (STRING)                                    |

Para um collector real, faça forwarder lendo `event_json` de execuções recentes. O framework não emite HTTP por padrão.

### 9.10 `ctrl_ingestion_metadata`

Uma linha por componente de controle, usada para auditoria de versão.

| Campo                                                        |
| ------------------------------------------------------------ |
| `component`, `framework_version`, `ctrl_schema_version`      |
| `updated_at_utc`                                             |

---

## 10. Locks, retry e idempotência

### 10.1 Modelo de concorrência

- **Delta optimistic concurrency** é a fonte de verdade. Conflitos resultam em `ConcurrentAppendException` ou similares, capturados pelo `with_retry`.
- **`lock_enabled`** é uma camada de **redução de colisão**, não de garantia. Um lock só impede que **dois jobs cooperando com o framework** rodem ao mesmo tempo na mesma target. Jobs externos não respeitam o lock.
- TTL do lock (default 120 minutos) garante limpeza automática se um job morrer sem `release_lock`.

### 10.2 Idempotência por modo

| Modo                   | Re-execução com mesma fonte |
| ---------------------- | --------------------------- |
| `scd0_append`          | ❌ duplica (use watermark)   |
| `scd0_overwrite`       | ✅ resultado idêntico        |
| `scd1_upsert`          | ✅                           |
| `scd1_hash_diff`       | ✅ (hash detecta no-op)      |
| `scd2_historical`      | ✅ (hash igual → no INSERT)  |
| `snapshot_soft_delete` | ✅                           |

### 10.3 Recuperação de falhas

- Se `ingest_plan` falha **antes** de `_execute_write_mode`, target intacto.
- Se falha **durante** `_execute_write_mode`, Delta fez rollback (transação atômica). `delta_version_before == delta_version_after`, `write_committed=False`.
- Se falha **após** `_execute_write_mode` mas **antes** de `upsert_state`: target tem dados, mas `state` não foi atualizado. Próxima execução vê mesmo `wm_prev` → re-tenta. Para modos não-idempotentes (`scd0_append`), isso duplica; para os demais, é no-op. A próxima execução também atualizará `state` corretamente.
- O `try/except/finally` garante que `runs` recebe o registro mesmo em falha catastrófica.

---

## 11. Lineage OpenLineage e Explain

Ver §4.10. Padrões de uso:

### 11.1 Habilitar em silver/gold

```python
ingest(
    ...,
    openlineage_enabled=True,
    openlineage_namespace="databricks://prod_catalog",
    openlineage_producer="my-org/lakehouse/v1",
)
```

### 11.2 Forwarder para Marquez

Pseudocódigo de um job auxiliar:

```python
events = spark.read.table("ops.ctrl_ingestion_lineage") \
    .where("event_time_utc >= current_timestamp() - INTERVAL 5 MINUTES") \
    .collect()
for row in events:
    requests.post(MARQUEZ_URL + "/api/v1/lineage", json=json.loads(row.event_json))
```

### 11.3 Explain

`explain_mode=True` é caro em datasets grandes (compila o plano completo). Use em desenvolvimento/CI, não em produção contínua. Plano fica em `ctrl_ingestion_explain.plan_text`.

---

## 12. Configuração e parâmetros

### 12.1 Por chamada (preferido)

Tudo via `IngestionPlan` (campos imutáveis) ou kwargs de `ingest()`.

### 12.2 Globais (`FrameworkConfig`)

A instância singleton `lakehouse_ingestion.config.CONFIG` contém defaults. Você pode trocá-la **antes** da primeira chamada:

```python
import lakehouse_ingestion.config as cfg
cfg.CONFIG = cfg.FrameworkConfig(
    ctrl_schema="my_ops",
    default_lock_ttl_minutes=60,
    max_inline_accepted_values=2000,
)
```

Como `CONFIG` é usado por outros módulos via `from .config import CONFIG`, monkey-patch precisa ser feito **antes** de outros imports — em prática, mais simples passar `ctrl_schema=` no plan.

### 12.3 Variáveis de ambiente

O pacote em si não lê env vars. A fixture de teste lê `SKIP_SPARK_TESTS=1` para pular testes de integração.

---

## 13. Testes

### 13.1 Estrutura

```
tests/
├── conftest.py     # SparkSession + Delta com skip gracioso
├── test_plan.py    # 11 testes puros
├── test_quality.py # quality gates
├── test_watermark.py
├── test_schema.py
└── test_modes.py   # 6 modos + dry run + bronze + watermark
```

### 13.2 Conftest — fixture `spark`

Levanta uma `SparkSession` local com Delta:

```python
SparkSession.builder
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ...
```

Tenta `delta.configure_spark_with_delta_pip(builder)` se a função existir.

**Skip gracioso:** se `SparkSession.builder.getOrCreate()` falhar (ex.: sem Java), o teste é pulado com mensagem clara. Nenhum erro de import quebra o coletor.

A fixture **injeta a sessão** no resolver do framework:

```python
from lakehouse_ingestion import _spark as spark_module
spark_module._cached_session = sess
```

Assim os testes usam exatamente essa sessão.

### 13.3 Tipos de teste

- **Puros**: validação de `IngestionPlan`, `validate_write_mode`, `normalize_quality_rules`, parsing de listas. Rodam em qualquer máquina com Python.
- **Spark**: criam DataFrames pequenos (3-10 linhas) e validam comportamento ponta-a-ponta.

### 13.4 Rodando

```bash
pip install -e ".[dev]"
pytest                      # tudo
pytest tests/test_plan.py   # só puros
SKIP_SPARK_TESTS=1 pytest   # força skip de Spark
pytest -k "scd2"            # filtro por nome
```

Status local validado: suite completa com `152 passed` usando Python 3.11, PySpark 3.5.x,
delta-spark 3.x e Java disponível. Use `SKIP_SPARK_TESTS=1` apenas quando o host não
tiver runtime Spark/Delta funcional.

### 13.5 Cobertura conceitual

| Aspecto                 | Coberto por                                          |
| ----------------------- | ---------------------------------------------------- |
| Construção de plan      | `test_plan.py`                                       |
| Quality consolidado     | `test_quality.py::test_combined_rules_single_pass`   |
| Watermark composto      | `test_watermark.py::test_apply_watermark_composite`  |
| Schema policy           | `test_schema.py::test_validate_schema_policy_*`      |
| Hash determinístico     | `test_schema.py::test_add_row_hash_is_deterministic` |
| Dedup por order         | `test_schema.py::test_dedup_by_order_keeps_latest`   |
| 6 modos de escrita      | `test_modes.py::test_<mode>_*`                       |
| Dry run                 | `test_modes.py::test_dry_run_does_not_write`         |
| Bronze restriction      | `test_modes.py::test_bronze_rejects_scd1_upsert`     |
| Quality fail/quarantine | `test_modes.py::test_quality_*`                      |
| Watermark integrado     | `test_modes.py::test_watermark_filters_already_seen` |

---

## 14. Deploy e empacotamento

### 14.1 Build

```bash
pip install build twine
python -m build
twine check dist/*
```

Gera `dist/contractforge-1.15.0-py3-none-any.whl` e `.tar.gz`.

### 14.2 Instalação no Databricks

Resumo (mais detalhes na resposta sobre instalação no chat):

1. **Cluster library a partir de UC Volume**: suba o `.whl` para `/Volumes/<cat>/<sch>/libs/`, instale via Compute → Libraries. Reinicie o cluster. Usar para uso compartilhado.
2. **Notebook-scoped** (`%pip install /Volumes/.../*.whl`): para iteração e serverless.
3. **Job cluster libraries** (DAB ou UI): para jobs específicos.
4. **PyPI privado**: se o time tem index, é o caminho mais limpo.

Não instale como workspace file — perde o gerenciamento de deps e versionamento.

### 14.3 Compatibilidade

- Python 3.10+.
- PySpark 3.4+ e delta-spark 3.0+ quando fora do Databricks. Instale com `pip install ".[spark]"`.
- Em Databricks, Spark e Delta são fornecidos pelo runtime; o wheel não declara essas dependências como obrigatórias para evitar resolução desnecessária no serverless.

### 14.4 Migração de versão futura

- Reseguir contrato de `IngestionPlan` é prioridade. Mudanças aditivas são OK; remoções/renames exigem major bump.
- Schemas das ctrl tables podem evoluir; documente migração no CHANGELOG.
- Compatibilidade reversa de watermark JSON é mantida sempre que possível.

---

## 15. Decisões de design

### 15.1 Por que dataclass frozen para `IngestionPlan`?

- Imutabilidade evita bugs por mutação acidental em código distribuído (mesmo plan passado a múltiplos jobs).
- `dataclass(frozen=True)` é trivial e auto-documenta (hash automático, comparação, repr).
- Não precisamos de validação cruzada sofisticada — `_validate_plan` faz isso no orquestrador.

### 15.2 Por que helpers SQL em vez de query builder?

Queries pontuais e conhecidas. Builder seria over-engineering. A separação `q`/`sql_lit` faz a higiene mínima necessária.

### 15.3 Por que single-pass quality em vez de framework de expectations?

- Casos de uso reais para gates simples são >90% das regras (`not_null`, `unique`, `accepted_values`, `min_rows`).
- Single-pass tem custo previsível.
- Para regras complexas, recomenda-se Great Expectations / Soda fora do framework.

### 15.4 Por que watermark JSON tipado?

- Permite watermark de tipos não-string (timestamp, bigint) preservando precisão.
- Watermark composto fica natural (struct → JSON object).
- JSON é debugável, diffável, extensível.

### 15.5 Por que ctrl tables Delta em vez de tabela externa?

- Mantém tudo no mesmo Lakehouse. Auditoria é só uma query Spark.
- Particionamento de `ctrl_ingestion_runs` por `run_date` permite VACUUM e retenção fáceis.
- Sem dependência externa (não precisa de Postgres/Redis/etc).

### 15.6 Por que NÃO instrumentação OpenTelemetry?

- Reduz dependências. Para times que querem OTel, é fácil envolver `ingest_plan` num span externo.
- Eventos OpenLineage cobrem o caso de "lineage tracking", que é o foco principal.

### 15.7 Por que separar `ingestion.py` em submódulos?

- Arquivo monolítico de 1800 linhas dificulta:
  - revisão (PR ficam imensos);
  - cobertura (difícil ver o que falta testar);
  - reúso (tudo era importável só do orquestrador);
  - localização de bug (cada feature ficava embolada).
- Modularização não muda API pública e melhora cada um desses pontos.

### 15.8 Por que NUL byte (``) como sentinela de NULL no hash?

- NUL não aparece em texto válido (UTF-8/Latin1) — colisão com dados reais é praticamente zero.
- Distingue `NULL` de string vazia: `coalesce(col, "")` faria `NULL` virar `""` e colidir com strings vazias originais.

### 15.9 Por que Unit Separator (``) como delimitador de hash?

- Caractere de controle ASCII reservado historicamente para esse fim.
- Resistente a colisões: `concat_ws("|", "a|b", "c")` daria `"a|b|c"`, igual a `concat_ws("|", "a", "b|c")`. Com `` essa colisão não acontece em dados normais.

### 15.10 Por que erro em `snapshot_soft_delete + watermark/filter`?

- `snapshot_soft_delete` define que o source é o estado completo atual.
- `watermark_columns` e `filter_expression` tornam o source parcial. Para `ConnectorSpec`, snapshot exige `source.read.source_complete=true` ou `source.read.full_snapshot=true`.
- Source parcial faria linhas fora do recorte virarem `is_active=false` incorretamente.
- Por isso a validação falha cedo com `ValueError`, em vez de emitir warning.
- A decisão completa está registrada em [ADR-003](./adrs/ADR-003-snapshot-soft-delete-sql-merge.md).

---

## 16. Glossário

- **Bronze / Silver / Gold** — convenção do Lakehouse Medallion: bronze = raw imutável, silver = limpo/conformado, gold = pronto para consumo analítico.
- **`ingest()` vs `ingest_plan()`** — `ingest` é wrapper procedural que constrói `IngestionPlan` a partir de kwargs e delega; `ingest_plan` recebe o plano direto.
- **SCD** — Slowly Changing Dimension. Tipo 0: não-mudança/append; tipo 1: sobrescreve; tipo 2: histórico.
- **Watermark** — marca de progresso temporal (ou ordinal) que define "até onde já processei". Pode ser composto (várias colunas em ordem lexicográfica).
- **Quality gate** — regra que decide se a escrita pode acontecer. No framework: `fail`, `warn` ou `quarantine`.
- **Quarentena** — destino de linhas que falharam regras isoláveis (não-duplicatas). Vira JSON em `ctrl_ingestion_quarantine`.
- **`row_hash`** — hash SHA-256 das colunas não-controle, em ordem alfabética, com separador NUL/US. Determinístico.
- **Hash diff** — modo de escrita SCD1 que só insere quando o `row_hash` difere do "atual" no target.
- **Soft delete** — linha marcada `is_active=false` com `deleted_at=now()` em vez de DELETE físico. Permite auditoria.
- **`change_columns` em SCD2** — colunas cuja mudança gera nova versão. Outras colunas mudam in-place na versão atual? **Não** — o hash é só sobre `change_columns`, então mudança fora de `change_columns` é ignorada (não vira nova versão e a antiga não é atualizada).
- **`replaceWhere`** — opção do Delta para overwrite seletivo por predicado. Usado em `scd0_overwrite` por partição.
- **`whenNotMatchedBySource`** — cláusula MERGE Delta para linhas presentes no target mas ausentes na source. Base do `snapshot_soft_delete`.
- **OpenLineage** — padrão aberto de eventos de lineage; emitido em `ctrl_ingestion_lineage` como JSON.
- **Best-effort lock** — lock que reduz colisão mas não garante exclusão mútua estrita (janela de corrida documentada).
- **Optimistic concurrency** — modelo Delta: transações cometem assumindo que não há conflito; em conflito, uma falha e o framework retenta (`with_retry`).

---

**Fim do documento.**
Atualize com mudanças em PRs e mantenha o sumário sincronizado.
