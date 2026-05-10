# Lakehouse Ingestion Framework

Framework de ingestão para Databricks e Delta Lake, com contratos declarativos por tabela, suporte a Bronze/Silver/Gold, quality gates, watermarks tipados, SCD, snapshot com soft delete, explain mode e emissão de eventos OpenLineage em JSON.

Documentação:
- [docs/guia_de_uso.md](docs/guia_de_uso.md) — passo a passo prático para testar como pacote ou script, padrão YAML + notebook genérico, orquestração com `for_each_task` e com master, troubleshooting e FAQ.
- [docs/arquitetura.md](docs/arquitetura.md) — referência técnica detalhada de cada submódulo, fluxo de execução, esquemas das ctrl tables e decisões de design (~70 KB).
- [docs/oficial.md](docs/oficial.md) — documentação original do framework (45 KB).

## Instalação local

```bash
pip install .
```

Para desenvolvimento e testes:

```bash
pip install -e ".[dev]"
```

## Build para PyPI

```bash
python -m pip install build twine
python -m build
twine check dist/*
```

## Requisitos de runtime

- Python 3.10+
- PySpark 3.4+ (ou Databricks Runtime equivalente)
- delta-spark 3.0+
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
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={"not_null": ["id_cliente"], "unique_key": ["id_cliente"]},
    explain_mode=True,
    openlineage_enabled=True,
)
```

## Modos oficiais

- `scd0_append`: inserção imutável.
- `scd0_overwrite`: substituição total ou por partição.
- `scd1_upsert`: atualização do estado atual por chaves.
- `scd1_hash_diff`: inserção apenas de versões novas ou alteradas por hash.
- `scd2_historical`: histórico completo com `valid_from`, `valid_to` e `is_current`. Reaparições de chaves não correntes criam uma nova versão atual.
- `snapshot_soft_delete`: sincronização por snapshot com marcação de ausentes em `is_active` e `deleted_at`. Exige source completo — o framework rejeita com `ValueError` quando combinado com `watermark_columns` ou `filter_expression`.

## Quality gates

Definidas via parâmetro `quality_rules` (dict ou `QualityRules`):

- `required_columns`, `not_null`, `unique_key`, `accepted_values`, `min_rows`, `max_null_ratio`.
- A avaliação consolida regras de coluna numa única agregação para reduzir I/O em datasets grandes.
- A ação em falha (`on_quality_fail`) pode ser:
  - `fail` (padrão): aborta a execução.
  - `warn`: registra mas escreve tudo.
  - `quarantine`: linhas problemáticas vão para `ctrl_ingestion_quarantine`; o restante é gravado e `effective_rows = rows_read - rows_quarantined`. **Vale apenas para regras de linha** (`not_null`, `accepted_values`, `max_null_ratio`). Regras de conjunto (`unique_key`, `min_rows`, `required_columns`) não têm como isolar linhas e escalam automaticamente para `fail`.

## Schema policy

- `permissive`: permite adições, remoções e mudanças de tipo.
- `additive_only`: aceita colunas novas, rejeita remoções/mudanças de tipo.
- `strict`: rejeita qualquer divergência.

Em `permissive` e `additive_only`, colunas novas são adicionadas ao Delta target via `ALTER TABLE`.

## Observabilidade

O framework cria tabelas de controle no schema configurado:

- `ctrl_ingestion_runs`
- `ctrl_ingestion_state`
- `ctrl_ingestion_quality`
- `ctrl_ingestion_quarantine`
- `ctrl_ingestion_locks`
- `ctrl_ingestion_explain`
- `ctrl_ingestion_lineage`

`explain_mode=True` captura o plano Spark do DataFrame preparado.

`openlineage_enabled=True` grava um evento OpenLineage em JSON na tabela de lineage.

## Estrutura do pacote

```
src/lakehouse_ingestion/
├── __init__.py        # Façade pública (ingest, ingest_plan, IngestionPlan, QualityRules, FrameworkConfig)
├── _spark.py          # Resolução de SparkSession + safe_cache/serverless
├── _sql.py            # Helpers de identificadores e literais SQL
├── config.py          # FrameworkConfig, tipos e constantes
├── plan.py            # IngestionPlan, QualityRules, build_plan_from_kwargs
├── schema.py          # hash, dedup, encoding, schema policy
├── watermark.py       # watermark simples e composto, encode/decode/apply
├── quality.py         # quality gates (avaliação consolidada) + quarentena
├── state.py           # ctrl tables, log, upsert state, locks, retry
├── writers.py         # motores por modo (append, overwrite, upsert, hash diff, snapshot, scd2)
├── lineage.py         # explain capture e OpenLineage
└── ingestion.py       # orquestrador (ingest, ingest_plan)
```

## Testes

```bash
pip install -e ".[dev]"
pytest
```

A suíte tem dois grupos:

- **Testes puros** (rápidos, sem Spark): validações de plano e parsing.
- **Testes integrados com Spark + Delta**: 6 modos de escrita, quality gates, watermark, schema policy. Pulam graciosamente em hosts sem Java/JDK; em CI com JDK 11+ rodam normalmente.

Variável `SKIP_SPARK_TESTS=1` força o pulo dos testes integrados.
