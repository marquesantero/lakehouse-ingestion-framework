# Lakehouse Ingestion Framework

**Documentação oficial**  
**Versão da biblioteca:** `1.0.0`  
**Pacote:** `lakehouse-ingestion-framework`  
**Import principal:** `lakehouse_ingestion`  
**Ambiente-alvo:** Databricks, Unity Catalog e Delta Lake  
**Licença:** MIT

---

## 1. Finalidade

O Lakehouse Ingestion Framework é uma biblioteca Python para padronizar ingestões em Delta Lake no Databricks. A biblioteca organiza padrões recorrentes de ingestão, escrita, controle operacional, qualidade, evolução de schema, watermarks, histórico SCD, snapshot com soft delete, planos de execução e eventos de linhagem.

O objetivo não é substituir orquestradores como Databricks Workflows, Airflow ou Databricks Asset Bundles. O framework atua dentro do job ou notebook, fornecendo um contrato único de execução para tabelas Bronze, Silver e Gold.

A biblioteca foi desenhada para cenários em que múltiplos pipelines precisam seguir o mesmo comportamento operacional, evitando variações de implementação entre analistas, engenheiros e projetos.

---

## 2. Escopo

O framework cobre:

- Leitura de origem a partir de tabela Unity Catalog ou `DataFrame` Spark.
- Escrita Delta nos modos `scd0_append`, `scd0_overwrite`, `scd1_upsert`, `scd1_hash_diff`, `scd2_historical` e `snapshot_soft_delete`.
- Controle de execução em tabelas Delta no schema operacional.
- Watermarks simples ou compostos com preservação de tipo.
- Validação de schema com políticas `permissive`, `additive_only` e `strict`.
- Quality gates com ação `fail`, `warn` ou `quarantine`.
- Quarentena de registros inválidos.
- Retry para conflitos concorrentes do Delta.
- Lock operacional best-effort.
- Captura de plano físico/lógico via `explain_mode`.
- Emissão de evento OpenLineage em JSON.
- Métricas de operação Delta extraídas do histórico da tabela.
- Otimização Delta opcional via `OPTIMIZE` e `ZORDER`.

O framework não cobre nativamente, nesta versão:

- Structured Streaming.
- CDC baseado em Change Data Feed como origem primária.
- Lock distribuído pessimista com garantia absoluta.
- Orquestração de DAGs.
- Gerenciamento de permissões Unity Catalog.
- Testes de qualidade complexos com expressões SQL arbitrárias.

---

## 3. Instalação

### 3.1 Instalação via PyPI

```bash
pip install lakehouse-ingestion-framework
```

### 3.2 Instalação em Databricks

Opções comuns:

1. Instalar o pacote como biblioteca de cluster.
2. Instalar o pacote como biblioteca de job.
3. Instalar o wheel gerado em um workspace ou volume acessível pelo job.
4. Usar `%pip install lakehouse-ingestion-framework` no início do notebook.

Exemplo em notebook Databricks:

```python
%pip install lakehouse-ingestion-framework
```

Após a instalação:

```python
from lakehouse_ingestion import ingest, ingest_plan, IngestionPlan, QualityRules, FrameworkConfig
```

---

## 4. Pré-requisitos

| Item | Recomendação |
|---|---|
| Python | `>= 3.10` |
| PySpark | `>= 3.4` |
| Delta Lake | `>= 3.0` |
| Databricks Runtime | DBR 13.3 LTS ou superior. DBR 14+ recomendado. |
| Unity Catalog | Recomendado para catálogo, schema, permissões e governança. |
| Permissões | `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`, `CREATE TABLE`, `SELECT`, `MODIFY` conforme o destino. |

A biblioteca assume execução em ambiente Spark com sessão `spark` disponível. Em Databricks, isso normalmente é fornecido pelo runtime.

---

## 5. Modelo conceitual

### 5.1 Arquitetura Medallion

A biblioteca usa o parâmetro `layer` para definir a camada lógica:

| Camada | Finalidade | Modos comuns |
|---|---|---|
| `bronze` | Captura da origem, preservação, rastreabilidade e baixa intervenção. | `scd0_append`, `scd0_overwrite`, `scd1_hash_diff` quando houver contrato explícito. |
| `silver` | Padronização, deduplicação, qualidade, consolidação e histórico. | `scd1_upsert`, `scd1_hash_diff`, `scd2_historical`, `snapshot_soft_delete`. |
| `gold` | Tabelas de consumo, agregações, métricas e modelos semânticos. | `scd0_overwrite`, `scd1_upsert`, eventualmente `scd0_append` para fatos. |

A validação interna restringe o uso de `scd1_upsert`, `scd2_historical` e `snapshot_soft_delete` na Bronze. Bronze deve ser orientada a captura, não a modelagem histórica.

### 5.2 Nome completo da tabela alvo

A tabela alvo é montada como:

```text
{catalog}.{layer}.{target_table}
```

Exemplo:

```python
ingest(
    source=df,
    catalog="sandbox_catalog1",
    layer="silver",
    target_table="c_cliente",
    mode="scd1_upsert",
    merge_keys="id_cliente"
)
```

Destino final:

```text
sandbox_catalog1.silver.c_cliente
```

---

## 6. Modos de escrita

### 6.1 Resumo

| Modo | Tipo | Comportamento | Chaves obrigatórias | Colunas técnicas adicionadas |
|---|---|---|---|---|
| `scd0_append` | SCD0 | Insere dados sem atualizar registros anteriores. | Não | `ingestion_date`, `source_system`, `__run_id` |
| `scd0_overwrite` | SCD0 | Substitui a tabela ou uma partição. | Não | `ingestion_date`, `source_system`, `__run_id` |
| `scd1_upsert` | SCD1 | Atualiza o estado atual por chave natural. | `merge_keys` | `ingestion_date`, `source_system`, `__run_id` |
| `scd1_hash_diff` | SCD1 append-oriented | Insere apenas registros novos ou alterados por comparação de hash. | `hash_keys` | `row_hash`, `ingestion_date`, `source_system`, `__run_id` |
| `scd2_historical` | SCD2 | Mantém versões históricas com intervalo de validade. | `merge_keys` | `row_hash`, `valid_from`, `valid_to`, `is_current`, `changed_columns` |
| `snapshot_soft_delete` | Snapshot CDC | Sincroniza snapshot completo e marca ausentes como inativos. | `merge_keys` | `row_hash`, `is_active`, `deleted_at` |

### 6.2 `scd0_append`

Uso indicado para eventos, logs, fatos transacionais e cargas append-only.

Características:

- Não faz comparação com destino.
- Não atualiza registros existentes.
- Permite evolução de schema conforme `schema_policy`.
- É o modo padrão da biblioteca.

Exemplo:

```python
ingest(
    source="raw_orders",
    catalog="main",
    layer="bronze",
    target_table="b_orders",
    mode="scd0_append",
    watermark_columns="updated_at",
    source_system="erp"
)
```

### 6.3 `scd0_overwrite`

Uso indicado para tabelas de referência, snapshots pequenos e reprocessamentos controlados.

Características:

- Pode substituir a tabela inteira.
- Pode substituir apenas uma partição com `partition_column` e `partition_value`.
- Quando usado com Liquid Clustering, a escrita não deve misturar `partitionBy` físico no mesmo destino.

Exemplo com substituição total:

```python
ingest(
    source=df_ref,
    catalog="main",
    layer="silver",
    target_table="c_status_pedido",
    mode="scd0_overwrite",
    schema_policy="strict"
)
```

Exemplo com substituição por partição:

```python
ingest(
    source=df_mes,
    catalog="main",
    layer="gold",
    target_table="faturamento_mensal",
    mode="scd0_overwrite",
    partition_column="competencia",
    partition_value="2026-05"
)
```

### 6.4 `scd1_upsert`

Uso indicado para manter o estado atual de uma entidade sem preservar histórico completo.

Características:

- Usa `MERGE INTO` Delta.
- Requer `merge_keys`.
- Atualiza colunas não-chave quando a chave já existe.
- Insere novos registros quando a chave não existe.
- Pode limitar o escopo do merge com `merge_strategy="delta_by_partition"` e `merge_partition_column`.

Exemplo:

```python
ingest(
    source="b_cliente",
    catalog="main",
    layer="silver",
    target_table="c_cliente",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only"
)
```

### 6.5 `scd1_hash_diff`

Uso indicado quando a origem não fornece CDC confiável, mas é necessário evitar inserir versões idênticas.

Características:

- Calcula `row_hash` sobre as colunas de negócio.
- Exclui colunas técnicas e colunas configuradas em `hash_exclude_columns`.
- Compara o hash atual com a última versão conhecida por `hash_keys`.
- Usa `dedup_order_expr` quando informado. Sem expressão explícita, usa `ingestion_sequence` ou `ingestion_ts_utc`; targets legados com múltiplas versões por chave e sem ordenação confiável falham com mensagem objetiva.
- Evita operações `UPDATE` em larga escala, privilegiando escrita append-only.
- Pode reduzir leitura do target quando `partition_column` está presente.

Exemplo:

```python
ingest(
    source="b_produto",
    catalog="main",
    layer="silver",
    target_table="c_produto_versions",
    mode="scd1_hash_diff",
    hash_keys="id_produto",
    hash_exclude_columns="updated_at|source_file",
    dedup_order_expr="updated_at DESC NULLS LAST",
    partition_column="ingestion_date"
)
```

### 6.6 `scd2_historical`

Uso indicado para preservar histórico completo de alterações de uma entidade.

Características:

- Requer `merge_keys`.
- Fecha a versão corrente quando há alteração relevante.
- Insere nova versão corrente com `is_current=true`.
- Usa `valid_from` e `valid_to`.
- Pode rastrear colunas alteradas em `changed_columns`.
- Quando `scd2_change_columns` é informado, o hash de mudança considera apenas essas colunas.
- Quando `scd2_change_columns` não é informado, o hash considera colunas de negócio, excluindo chaves e colunas técnicas.

Semântica de reativação:

- Se uma chave previamente inativa reaparecer, o framework cria uma nova versão histórica corrente.
- Versões anteriores permanecem encerradas com `is_current=false`.
- A biblioteca não “revive” fisicamente a versão antiga. Isso preserva rastreabilidade histórica.

Exemplo:

```python
ingest(
    source="c_cliente",
    catalog="main",
    layer="silver",
    target_table="dim_cliente_historico",
    mode="scd2_historical",
    merge_keys="id_cliente",
    scd2_change_columns="nome|email|status|cidade",
    scd2_effective_from_column="updated_at",
    cluster_columns="id_cliente|status",
    schema_policy="additive_only"
)
```

### 6.7 `snapshot_soft_delete`

Uso indicado quando a origem envia um snapshot completo do estado atual e registros ausentes devem ser tratados como inativos.

Características:

- Requer `merge_keys`.
- Atualiza registros existentes quando o `row_hash` muda.
- Insere novos registros.
- Marca registros ausentes no snapshot como `is_active=false`.
- Preenche `deleted_at` com o timestamp da execução.
- Pressupõe snapshot completo da entidade. **Não pode ser combinado com `watermark_columns` ou `filter_expression`** — o framework rejeita com `ValueError` no `_validate_plan`. Para sincronização incremental, use `scd1_upsert`.
- Em Databricks Serverless, executa `MERGE` SQL para evitar a API Python `DeltaTable`, que pode não estar disponível em Spark Connect.

Exemplo:

```python
ingest(
    source=df_snapshot_clientes,
    catalog="main",
    layer="silver",
    target_table="c_cliente_snapshot",
    mode="snapshot_soft_delete",
    merge_keys="id_cliente",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only"
)
```

---

## 7. API pública

A biblioteca expõe duas formas principais de execução:

```python
from lakehouse_ingestion import ingest, ingest_plan, IngestionPlan, QualityRules
```

### 7.1 `ingest(**kwargs)`

Função orientada a notebooks. Recebe parâmetros nomeados e cria internamente um `IngestionPlan`.

```python
result = ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id"
)
```

### 7.2 `ingest_plan(plan: IngestionPlan)`

Função orientada a configuração declarativa, testes e orquestração.

```python
plan = IngestionPlan(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys=["order_id"]
)

result = ingest_plan(plan)
```

### 7.3 Escolha recomendada

| Situação | Interface recomendada |
|---|---|
| Notebook exploratório | `ingest()` |
| Job padronizado | `ingest_plan()` |
| Configuração gerada por YAML/JSON | `ingest_plan()` |
| Testes unitários | `ingest_plan()` |
| Migração incremental de notebooks existentes | `ingest()` |

---

## 8. Referência completa de parâmetros

### 8.1 Identificação da execução

| Parâmetro | Tipo | Padrão | Obrigatório | Descrição |
|---|---:|---|---|---|
| `source` | `str | DataFrame` | sem padrão | Sim | Origem da ingestão. Pode ser nome de tabela ou DataFrame Spark. |
| `target_table` | `str` | sem padrão | Sim | Nome da tabela alvo sem catálogo e sem schema. O schema é definido por `layer`. |
| `catalog` | `str` | `"main"` | Não | Catálogo Unity Catalog onde alvo e tabelas de controle serão resolvidos. |
| `layer` | `"bronze" | "silver" | "gold"` | `"bronze"` | Não | Camada lógica usada como schema da tabela alvo. |
| `mode` | `WriteMode` | `"scd0_append"` | Não | Estratégia de escrita. |
| `source_system` | `str` | `"default"` | Não | Identificador da origem, gravado como metadado técnico. |
| `ctrl_schema` | `str` | `"ops"` | Não | Schema onde as tabelas de controle serão criadas. |
| `notebook_name` | `str` | `"unknown"` | Não | Nome lógico do notebook ou job para auditoria. |

### 8.2 Seleção, filtro e preparação

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `select_columns` | `str | List[str]` | `[]` | Colunas selecionadas da origem. Como string, usa `|` como separador. |
| `filter_expression` | `str | None` | `None` | Expressão Spark SQL aplicada com `where`. |
| `custom_keys` | `Dict[str, str | List[str]]` | `{}` | Cria colunas derivadas por concatenação de colunas naturais. |
| `dedup_order_expr` | `str | None` | `None` | Expressão de ordenação para deduplicação por `merge_keys` ou `hash_keys`. |
| `fix_encoding` | `bool` | `False` | Ativa correção de encoding em colunas string. |
| `encoding` | `str` | `"Windows-1252"` | Encoding de origem usado na correção. |
| `encoding_columns` | `str | List[str]` | `[]` | Colunas string onde a correção de encoding será aplicada. Se vazio, aplica em todas as strings. |

Exemplo com filtro, seleção e chave customizada:

```python
ingest(
    source="raw_items",
    target_table="c_item",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    select_columns="empresa|filial|pedido|item|updated_at|valor",
    filter_expression="updated_at IS NOT NULL",
    custom_keys={"id_item": "empresa|filial|pedido|item"},
    merge_keys="id_item",
    dedup_order_expr="updated_at DESC NULLS LAST"
)
```

### 8.3 Watermark

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `watermark_columns` | `str | List[str]` | `[]` | Coluna ou conjunto de colunas usado para carga incremental. |

O framework armazena watermarks em JSON com tipo Spark SQL. Isso permite comparar números como números, datas como datas e timestamps como timestamps.

Exemplo com watermark simples:

```python
ingest(
    source="raw_orders",
    target_table="b_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    watermark_columns="updated_at"
)
```

Exemplo com watermark composto:

```python
ingest(
    source="raw_movimentos",
    target_table="b_movimentos",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    watermark_columns="data_movimento|hora_movimento|seq_movimento"
)
```

Observações:

- A ordenação composta é lexicográfica por coluna, preservando o tipo original de cada coluna.
- Colunas com `NULL` podem impedir avanço correto. Em cargas incrementais, recomenda-se filtrar registros sem watermark.
- Mudança de tipo da coluna de watermark entre execuções pode causar falha de comparação ou rejeição pela política de schema.

### 8.4 Chaves e hash

| Parâmetro | Tipo | Padrão | Usado por | Descrição |
|---|---:|---|---|---|
| `merge_keys` | `str | List[str]` | `[]` | `scd1_upsert`, `scd2_historical`, `snapshot_soft_delete` | Chaves naturais usadas no `MERGE`. |
| `hash_keys` | `str | List[str]` | `[]` | `scd1_hash_diff` | Chaves usadas para comparar a versão mais recente no destino. |
| `hash_exclude_columns` | `str | List[str]` | `[]` | `scd1_hash_diff` e hash auxiliar | Colunas ignoradas no cálculo do hash. |
| `scd2_change_columns` | `str | List[str]` | `[]` | `scd2_historical` | Colunas usadas para detectar mudança histórica. |
| `scd2_effective_from_column` | `str | None` | `None` | `scd2_historical` | Coluna da origem usada como `valid_from`. Se omitida, usa timestamp da execução. |

Exemplo SCD2 com chave composta:

```python
ingest(
    source="c_preco_produto",
    target_table="dim_preco_produto",
    catalog="main",
    layer="silver",
    mode="scd2_historical",
    merge_keys="id_produto|id_tabela_preco",
    scd2_change_columns="preco|moeda|status",
    scd2_effective_from_column="updated_at"
)
```

### 8.5 Estratégia de escrita e layout Delta

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `partition_column` | `str | None` | `None` | Coluna de partição física Delta. Use com cuidado. |
| `partition_value` | `str | None` | `None` | Valor usado em overwrite por partição. |
| `merge_strategy` | `"delta" | "delta_by_partition" | "replace_partitions"` | `"delta"` | Estratégia aplicada em `scd1_upsert`. |
| `merge_partition_column` | `str | None` | `None` | Coluna usada para limitar merge por partições afetadas. |
| `cluster_columns` | `str | List[str]` | `[]` | Colunas usadas para Liquid Clustering. |
| `zorder_columns` | `str | List[str]` | `[]` | Colunas usadas no `OPTIMIZE ZORDER BY`. |
| `optimize_after_write` | `bool` | `False` | Executa `OPTIMIZE` após escrita com linhas gravadas. |

Recomendações:

- Prefira `cluster_columns` em Delta Lake moderno quando disponível.
- Evite partições físicas de alta cardinalidade, como UUID, timestamp completo ou IDs transacionais.
- Evite `OPTIMIZE` indiscriminado em microcargas ou tabelas pequenas.
- `OPTIMIZE` deve ser tratado como decisão de custo/performance, não como pós-processamento obrigatório.

Exemplo com Liquid Clustering:

```python
ingest(
    source="c_vendas",
    target_table="f_vendas",
    catalog="main",
    layer="gold",
    mode="scd1_upsert",
    merge_keys="id_venda",
    cluster_columns="dt_venda|id_cliente",
    optimize_after_write=True
)
```

### 8.6 Schema policy

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `schema_policy` | `"permissive" | "additive_only" | "strict"` | `"permissive"` | Política de evolução de schema. |

Comportamento:

| Política | Novas colunas | Colunas removidas | Mudança de tipo |
|---|---|---|---|
| `permissive` | Aceita | Aceita | Aceita conforme capacidade da escrita Delta. |
| `additive_only` | Aceita | Rejeita | Rejeita. |
| `strict` | Rejeita | Rejeita | Rejeita. |

Observações:

- Em modos com `MERGE`, novas colunas são sincronizadas antes do merge quando a política permite.
- Em `strict`, a origem precisa ter o mesmo contrato estrutural esperado pelo destino.
- Em `additive_only`, colunas novas são adicionadas ao destino, mas remoções e alterações de tipo falham cedo.

Exemplo:

```python
ingest(
    source="b_cliente",
    target_table="c_cliente",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    schema_policy="additive_only"
)
```

### 8.7 Quality gates

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `quality_rules` | `QualityRules | Dict | None` | `None` | Regras de qualidade executadas antes da escrita. |
| `on_quality_fail` | `"fail" | "warn" | "quarantine"` | `"fail"` | Ação quando regras falham. |
| `idempotency_key` | `str | None` | `None` | Chave lógica opcional do lote. |
| `skip_if_success` | `bool` | `False` | Se `True`, retorna `SKIPPED` quando já existe execução `SUCCESS` para o mesmo target e `idempotency_key`. |

Campos de `QualityRules`:

| Campo | Tipo | Descrição |
|---|---:|---|
| `required_columns` | `List[str]` | Colunas que devem existir no DataFrame. |
| `not_null` | `List[str]` | Colunas que não podem conter `NULL`. |
| `unique_key` | `List[str]` | Conjunto de colunas que deve ser único. |
| `accepted_values` | `Dict[str, List[Any]]` | Valores permitidos por coluna. Limitado por `CONFIG.max_inline_accepted_values`. |
| `min_rows` | `int | None` | Quantidade mínima de registros após preparação. |
| `max_null_ratio` | `Dict[str, float]` | Percentual máximo de nulos por coluna, entre 0 e 1. |
| `expressions` | `List[QualityExpression]` | Expressões SQL booleanas nomeadas. Valores `false` ou `NULL` falham. |

Ações:

| Ação | Comportamento |
|---|---|
| `fail` | Interrompe a execução e registra falha. |
| `warn` | Registra falhas, mas continua a escrita. |
| `quarantine` | Grava registros inválidos em quarentena e escreve apenas registros válidos. Vale para regras de linha (`not_null`, `accepted_values`, `max_null_ratio`); regras de conjunto (`unique_key`, `min_rows`, `required_columns`) não isolam linhas e escalam para `fail`. |

Exemplo com dicionário:

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
            {"name": "positive_amount", "expression": "amount > 0", "quarantine": True}
        ],
    },
    # unique_key, min_rows e required_columns são abort-only: a falha aborta
    # a execução. Para quarentena efetiva, remova-as e use regras de linha:
    # not_null, accepted_values, max_null_ratio ou expressions com quarantine=true.
    on_quality_fail="fail"
)
```

Exemplo com dataclass:

```python
from lakehouse_ingestion import QualityRules

rules = QualityRules(
    required_columns=["id_cliente", "updated_at"],
    not_null=["id_cliente"],
    unique_key=["id_cliente"],
    max_null_ratio={"email": 0.15}
)

ingest(
    source="b_cliente",
    target_table="c_cliente",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    quality_rules=rules,
    on_quality_fail="fail"
)
```

### 8.8 Execução, observabilidade e linhagem

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `dry_run` | `bool` | `False` | Prepara e valida a ingestão sem efeitos colaterais: não cria schemas/ctrl tables, não aplica `ALTER TABLE ADD COLUMNS`, não persiste em `ctrl_ingestion_quality`/`quarantine`/`runs`/`state`/`lineage`. Apenas as validações (schema policy, quality gates, watermark) executam. |
| `explain_mode` | `bool` | `False` | Captura `df.explain()` e persiste o resultado. |
| `explain_format` | `str` | `"formatted"` | Formato do explain. Valores comuns: `simple`, `extended`, `formatted`, `cost`, `codegen`. |
| `openlineage_enabled` | `bool` | `False` | Gera e persiste evento OpenLineage em JSON. |
| `openlineage_namespace` | `str | None` | `None` | Namespace usado no evento. Se omitido, usa catálogo/camada. |
| `openlineage_producer` | `str` | `"lakehouse-ingestion-framework"` | Identificador do produtor no evento OpenLineage. |
| `use_cache` | `bool` | `True` | Permite cache do DataFrame preparado. Desabilitado automaticamente em ambientes incompatíveis. |
| `lock_enabled` | `bool` | `False` | Ativa lock operacional best-effort. |

Exemplo com `dry_run`:

```python
result = ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id",
    dry_run=True
)

print(result)
```

Exemplo com explain e OpenLineage:

```python
ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id",
    explain_mode=True,
    explain_format="formatted",
    openlineage_enabled=True,
    openlineage_namespace="main.silver"
)
```

### 8.9 Integração com orquestradores

| Parâmetro | Tipo | Padrão | Descrição |
|---|---:|---|---|
| `parent_run_id` | `str | None` | `None` | ID de execução pai. Útil em DAGs ou jobs compostos. |
| `run_group_id` | `str | None` | `None` | ID lógico do grupo de execução. |
| `master_job_id` | `str | None` | `None` | ID do job mestre no orquestrador. |
| `master_run_id` | `str | None` | `None` | ID da execução mestre no orquestrador. |

Exemplo:

```python
ingest(
    source="b_items",
    target_table="c_items",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="item_id",
    parent_run_id=dbutils.widgets.get("parent_run_id"),
    run_group_id=dbutils.widgets.get("run_group_id"),
    master_job_id=dbutils.widgets.get("job_id"),
    master_run_id=dbutils.widgets.get("run_id")
)
```

---

## 9. Retorno da execução

A função retorna um dicionário com métricas e metadados.

| Campo | Descrição |
|---|---|
| `status` | `SUCCESS` ou `FAILED`. |
| `run_id` | Identificador único da execução. |
| `target_table` | Nome completo da tabela alvo. |
| `source_table` | Nome da origem ou `dataframe`. |
| `mode` | Modo de escrita executado. |
| `rows_read` | Quantidade de linhas após preparação. |
| `rows_written` | Quantidade de linhas consideradas na escrita. |
| `rows_quarantined` | Quantidade de registros enviados à quarentena. |
| `metrics_source` | Origem das métricas: `logical` ou `mixed`. |
| `watermark_previous` | Watermark antes da execução. |
| `watermark_current` | Watermark após a execução bem-sucedida. |
| `quality_status` | `PASSED`, `FAILED` ou `NOT_CONFIGURED`. |
| `schema_changes` | Diferenças de schema detectadas. |
| `operation_metrics` | Métricas obtidas no histórico Delta. |
| `write_committed` | Indica se houve commit Delta associado à escrita. |
| `delta_version_before` | Versão Delta antes da escrita. |
| `delta_version_after` | Versão Delta após a escrita. |
| `write_delta_version` | Versão Delta do commit de escrita, quando aplicável. |
| `explain_captured` | Indica se o explain foi capturado. |
| `openlineage_event_emitted` | Indica se o evento OpenLineage foi persistido. |
| `openlineage_event` | Evento OpenLineage em formato de dicionário. |
| `error_message` | Mensagem de erro truncada, quando houver falha. |

Exemplo de consumo:

```python
result = ingest(...)

if result["status"] != "SUCCESS":
    raise RuntimeError(result["error_message"])

print(result["rows_written"])
print(result["delta_version_after"])
```

---

## 10. Tabelas de controle

As tabelas são criadas automaticamente no schema definido por `ctrl_schema`, por padrão `ops`.

### 10.1 `ctrl_ingestion_runs`

Histórico completo de execuções.

Principais colunas:

| Coluna | Descrição |
|---|---|
| `run_id` | ID único da execução. |
| `run_ts_utc` | Timestamp lógico da execução. |
| `run_date` | Data UTC usada como partição. |
| `notebook_name` | Nome lógico do notebook ou job. |
| `layer` | Camada alvo. |
| `source_table` | Origem. |
| `target_table` | Destino. |
| `mode` | Modo de escrita. |
| `status` | Status final. |
| `rows_read` | Linhas lidas/preparadas. |
| `rows_written` | Linhas escritas. |
| `rows_inserted` | Linhas inseridas conforme métricas Delta. |
| `rows_updated` | Linhas atualizadas conforme métricas Delta. |
| `rows_deleted` | Linhas removidas ou marcadas conforme operação Delta. |
| `rows_quarantined` | Linhas enviadas à quarentena. |
| `watermark_previous` | Watermark anterior. |
| `watermark_current` | Watermark final. |
| `duration_seconds` | Duração total. |
| `quality_status` | Resultado dos quality gates. |
| `schema_policy` | Política de schema usada. |
| `schema_changes_json` | Diferenças estruturais detectadas. |
| `operation_metrics_json` | Métricas do histórico Delta. |
| `write_committed` | Indica se houve commit de escrita. |
| `delta_version_before` | Versão Delta antes. |
| `delta_version_after` | Versão Delta depois. |
| `error_message` | Erro truncado. |

Consulta útil:

```sql
SELECT
  run_date,
  target_table,
  mode,
  status,
  rows_read,
  rows_written,
  rows_quarantined,
  duration_seconds,
  delta_version_after
FROM main.ops.ctrl_ingestion_runs
ORDER BY run_ts_utc DESC;
```

### 10.2 `ctrl_ingestion_state`

Estado corrente por tabela alvo.

| Coluna | Descrição |
|---|---|
| `target_table` | Tabela alvo. |
| `watermark_column` | Colunas de watermark usadas. |
| `watermark_value` | Watermark atual serializado em JSON. |
| `last_success_at_utc` | Última execução bem-sucedida. |
| `last_run_id` | Último run associado. |
| `last_status` | Último status. |
| `last_rows_written` | Linhas escritas na última execução. |
| `last_delta_version` | Última versão Delta conhecida. |
| `last_write_completed_at_utc` | Momento de conclusão da escrita. |
| `last_watermark_candidate` | Watermark candidato calculado antes da escrita. |
| `last_updated_at_utc` | Última atualização do estado. |

Consulta útil:

```sql
SELECT
  target_table,
  last_status,
  watermark_column,
  watermark_value,
  last_delta_version,
  last_success_at_utc
FROM main.ops.ctrl_ingestion_state
ORDER BY last_updated_at_utc DESC;
```

### 10.3 `ctrl_ingestion_quality`

Resultado das regras de qualidade por execução.

| Coluna | Descrição |
|---|---|
| `run_id` | Execução. |
| `target_table` | Destino. |
| `rule_name` | Nome da regra. |
| `status` | Resultado da regra. |
| `failed_count` | Quantidade de falhas. |
| `checked_at_utc` | Momento da verificação. |
| `details_json` | Detalhes da regra. |

### 10.4 `ctrl_ingestion_quarantine`

Registros rejeitados quando `on_quality_fail="quarantine"`.

| Coluna | Descrição |
|---|---|
| `run_id` | Execução. |
| `target_table` | Destino lógico. |
| `rule_name` | Regra que enviou o registro para quarentena. |
| `error_reason` | Motivo serializado. |
| `record_payload` | Registro original em JSON. |
| `quarantined_at_utc` | Momento da quarentena. |

### 10.5 `ctrl_ingestion_metadata`

Tabela de uma linha por componente para registrar `framework_version`, `ctrl_schema_version` e `updated_at_utc`.
O framework aplica apenas migrações aditivas conhecidas com `ALTER TABLE ADD COLUMNS`; colunas nunca são removidas automaticamente.

### 10.5 `ctrl_ingestion_locks`

Reserva operacional best-effort por tabela alvo.

| Coluna | Descrição |
|---|---|
| `target_table` | Tabela protegida. |
| `run_id` | Execução que adquiriu o lock. |
| `acquired_at_utc` | Momento de aquisição. |
| `expires_at_utc` | Expiração por TTL. |
| `status` | `ACTIVE` ou `RELEASED`. |

O lock não é uma exclusão pessimista distribuída. Ele reduz colisões operacionais, mas a consistência final continua baseada no controle otimista do Delta Lake.

### 10.6 `ctrl_ingestion_explain`

Planos capturados quando `explain_mode=True`.

| Coluna | Descrição |
|---|---|
| `run_id` | Execução. |
| `target_table` | Destino. |
| `source_table` | Origem. |
| `mode` | Modo de escrita. |
| `explain_format` | Formato usado. |
| `plan_text` | Texto retornado por `df.explain`. |
| `captured_at_utc` | Momento da captura. |

### 10.7 `ctrl_ingestion_lineage`

Eventos OpenLineage em JSON.

| Coluna | Descrição |
|---|---|
| `run_id` | Execução. |
| `event_time_utc` | Momento do evento. |
| `event_type` | Tipo do evento. |
| `target_table` | Destino. |
| `source_table` | Origem. |
| `namespace` | Namespace OpenLineage. |
| `producer` | Produtor. |
| `event_json` | Evento completo em JSON. |

---

## 11. OpenLineage

Quando `openlineage_enabled=True`, a biblioteca emite um evento compatível com o modelo OpenLineage em JSON e o persiste em `ctrl_ingestion_lineage`.

A emissão é operacional e técnica. Ela registra execução, origem, destino, modo, métricas e schema quando disponível. Ela não substitui uma solução corporativa completa de catálogo, lineage semântico ou impacto regulatório.

Exemplo de ativação:

```python
ingest(
    source="b_orders",
    target_table="c_orders",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="order_id",
    openlineage_enabled=True,
    openlineage_namespace="main.silver",
    openlineage_producer="lakehouse-ingestion-framework"
)
```

Uso típico:

```sql
SELECT
  run_id,
  event_time_utc,
  event_type,
  source_table,
  target_table,
  namespace
FROM main.ops.ctrl_ingestion_lineage
ORDER BY event_time_utc DESC;
```

---

## 12. Explain mode

`explain_mode=True` captura o plano de execução do DataFrame preparado antes da escrita.

Exemplo:

```python
ingest(
    source="b_movimentos",
    target_table="c_movimentos",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_movimento",
    explain_mode=True,
    explain_format="formatted"
)
```

Consulta:

```sql
SELECT
  run_id,
  target_table,
  explain_format,
  plan_text
FROM main.ops.ctrl_ingestion_explain
WHERE target_table = 'main.silver.c_movimentos'
ORDER BY captured_at_utc DESC;
```

Observações:

- Em alguns ambientes Serverless, a captura de stdout pode variar conforme runtime.
- O explain representa o DataFrame preparado, não necessariamente todo o plano interno do Delta MERGE.
- Para análise de performance de MERGE, combine `ctrl_ingestion_explain` com `DESCRIBE HISTORY` da tabela alvo.

---

## 13. Retry e concorrência

A biblioteca aplica retry com backoff para erros compatíveis com concorrência Delta, como conflitos de commit.

O lock operacional pode ser ativado com:

```python
lock_enabled=True
```

Exemplo:

```python
ingest(
    source="b_cliente",
    target_table="c_cliente",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_cliente",
    lock_enabled=True
)
```

Garantias:

- O lock reduz a chance de dois jobs escreverem simultaneamente na mesma tabela alvo.
- O lock possui TTL para reduzir efeito de locks órfãos.
- O lock não substitui o modelo otimista do Delta Lake.
- Em alta concorrência, ainda pode haver conflito Delta, tratado por retry.

---

## 14. Garantias operacionais

| Operação | Garantia prática |
|---|---|
| `scd0_append` | Commit Delta atômico por escrita. |
| `scd0_overwrite` | Substituição Delta atômica. |
| `scd1_upsert` | MERGE transacional Delta. |
| `scd1_hash_diff` | Comparação por hash e append de diferenças detectadas. |
| `scd2_historical` | Atualização histórica em commit Delta único com staged rows. |
| `snapshot_soft_delete` | Sincronização por MERGE com marcação de ausentes. |
| Watermark | Avança somente após execução bem-sucedida registrada. |
| Quality gates | Executados antes da escrita. |
| Quarentena | Persistência dos registros inválidos antes da escrita dos válidos. |
| Locking | Best-effort, não pessimista. |
| OpenLineage | Evento operacional persistido em tabela Delta. |

---

## 15. Limitações conhecidas

- `snapshot_soft_delete` pressupõe snapshot completo da origem. Não use com carga parcial incremental.
- Locking é best-effort e não exclusão pessimista distribuída.
- Mudanças incompatíveis de tipo são rejeitadas em `additive_only` e `strict`.
- `explain_mode` pode ter limitações em alguns runtimes Serverless.
- Quality gates são intencionalmente simples. Regras complexas devem ser implementadas antes da chamada ou em camada especializada.
- `accepted_values` não deve ser usado com listas grandes. Para domínios grandes, use join com tabela de referência antes da ingestão.
- `OPTIMIZE` pode aumentar custo se executado após microcargas frequentes.
- A biblioteca não gerencia grants do Unity Catalog.
- A biblioteca não substitui testes automatizados de pipeline.

---

## 16. Exemplos completos

### 16.1 Bronze append incremental

```python
from lakehouse_ingestion import ingest

result = ingest(
    source="raw_erp_orders",
    target_table="b_erp_orders",
    catalog="main",
    layer="bronze",
    mode="scd0_append",
    source_system="erp",
    watermark_columns="updated_at",
    schema_policy="permissive",
    notebook_name="bronze_erp_orders"
)

print(result["status"])
```

### 16.2 Silver upsert com deduplicação e quarentena

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
        "required_columns": ["order_id", "updated_at", "status"],
        "not_null": ["order_id", "updated_at"],
        "unique_key": ["order_id"],
        "accepted_values": {"status": ["open", "closed", "cancelled"]}
    },
    # unique_key e required_columns são abort-only: a falha aborta a execução.
    # Para usar quarentena de fato, remova essas regras e mantenha apenas
    # not_null/accepted_values/max_null_ratio com on_quality_fail="quarantine".
    on_quality_fail="fail",
    explain_mode=True,
    openlineage_enabled=True
)
```

### 16.3 Silver SCD2 histórico

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
    openlineage_enabled=True
)
```

### 16.4 Snapshot com soft delete

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
        "unique_key": ["customer_id"]
    },
    on_quality_fail="fail"
)
```

### 16.5 Gold overwrite particionado

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
    optimize_after_write=True
)
```

### 16.6 Plano declarativo

```python
from lakehouse_ingestion import ingest_plan, IngestionPlan, QualityRules

plan = IngestionPlan(
    source="b_customer_events",
    target_table="c_customer_events",
    catalog="main",
    layer="silver",
    mode="scd1_upsert",
    merge_keys=["customer_id", "event_id"],
    watermark_columns=["event_ts"],
    dedup_order_expr="event_ts DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules=QualityRules(
        required_columns=["customer_id", "event_id", "event_ts"],
        not_null=["customer_id", "event_id"],
        unique_key=["customer_id", "event_id"]
    ),
    on_quality_fail="fail",  # unique_key/required_columns são abort-only
    explain_mode=True,
    openlineage_enabled=True,
    lock_enabled=True
)

result = ingest_plan(plan)
```

### 16.7 Dry run para validação operacional

```python
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
        "not_null": ["order_id"]
    },
    dry_run=True
)

display(result)
```

---

## 17. Troubleshooting

| Sintoma | Causa provável | Ação recomendada |
|---|---|---|
| `DELTA_CONCURRENT_APPEND` ou conflito de commit | Escritas concorrentes na mesma tabela ou partição. | Ative `lock_enabled`, reduza paralelismo no alvo ou ajuste dependências do workflow. |
| `MERGE source has multiple matches` | Duplicidade nas `merge_keys`. | Use `dedup_order_expr` e valide `unique_key` nos quality gates. |
| Watermark não avança | Linhas sem watermark, falha após escrita ou ausência de registros novos. | Verifique `ctrl_ingestion_state`, `watermark_candidate` e filtros de origem. |
| Tabela de quarentena vazia | Regras não falharam ou `on_quality_fail` não está como `quarantine`. | Consulte `ctrl_ingestion_quality`. |
| SCD2 gera muitas versões | `scd2_change_columns` amplo demais ou hash incluindo colunas voláteis. | Restrinja `scd2_change_columns` às colunas de negócio. |
| `accepted_values` falha com lista grande | Lista acima do limite configurado. | Use tabela de referência e join prévio. |
| `OPTIMIZE` aumenta custo sem melhorar performance | Tabela pequena ou microcargas frequentes. | Use política externa de otimização por volume/quantidade de arquivos. |
| Erro de permissão no schema `ops` | Usuário/job sem permissão para criar schema/tabelas. | Conceda permissões no catálogo ou crie previamente o schema operacional. |
| Explain vazio ou incompleto | Limitação de captura do runtime. | Consulte Spark UI e histórico Delta complementarmente. |
| Schema rejeitado em `additive_only` | Remoção ou alteração de tipo detectada. | Ajuste a origem, altere política conscientemente ou versionamento de contrato. |

---

## 18. Recomendações de uso por camada

### 18.1 Bronze

- Preferir `scd0_append`.
- Usar `schema_policy="permissive"` quando a origem é instável.
- Evitar regras de negócio complexas.
- Registrar `source_system` de forma consistente.
- Usar watermark quando a origem tiver coluna confiável.

### 18.2 Silver

- Usar `schema_policy="additive_only"` ou `strict`.
- Definir `merge_keys` ou `hash_keys` explicitamente.
- Sempre configurar deduplicação quando houver risco de múltiplos registros por chave.
- Usar quality gates em entidades críticas.
- Usar `scd2_historical` somente quando histórico for necessário.

### 18.3 Gold

- Preferir contratos estáveis e `schema_policy="strict"`.
- Evitar mudanças automáticas de schema.
- Controlar granularidade da tabela e semântica das métricas fora da biblioteca.
- Usar `scd0_overwrite` para reconstruções controladas.
- Evitar `OPTIMIZE` automático sem avaliação de custo.

---

## 19. Empacotamento e publicação

Estrutura esperada do pacote:

```text
lakehouse_ingestion_pkg/
├── pyproject.toml
├── README.md
└── src/
    └── lakehouse_ingestion/
        ├── __init__.py
        └── ingestion.py
```

Exemplo de `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "lakehouse-ingestion-framework"
version = "1.0.0"
description = "Framework de ingestão Delta Lake para Databricks com contratos declarativos, quality gates, SCD, explain mode e eventos OpenLineage."
readme = "README.md"
requires-python = ">=3.10"
license = {text = "MIT"}
dependencies = [
    "pyspark>=3.4",
    "delta-spark>=3.0"
]

[project.optional-dependencies]
databricks = ["databricks-sdk>=0.20"]
dev = ["build>=1.0", "twine>=4.0", "pytest>=7.0", "ruff>=0.4"]
```

Build local:

```bash
python -m pip install --upgrade build twine
python -m build
python -m twine check dist/*
```

Publicação em TestPyPI:

```bash
python -m twine upload --repository testpypi dist/*
```

Publicação em PyPI:

```bash
python -m twine upload dist/*
```

---

## 20. Checklist antes de produção

| Item | Status esperado |
|---|---|
| Catálogo e schemas criados ou com permissão de criação. | Validado. |
| Schema `ops` com permissão para tabelas de controle. | Validado. |
| Modos de escrita definidos por tabela. | Validado. |
| Chaves naturais revisadas. | Validado. |
| Watermarks testados com `dry_run=True`. | Validado. |
| Quality gates definidos para Silver crítica. | Validado. |
| SCD2 limitado às colunas de mudança relevantes. | Validado. |
| `snapshot_soft_delete` usado apenas com snapshot completo. | Validado. |
| Concorrência avaliada por tabela alvo. | Validado. |
| `explain_mode` testado em desenvolvimento. | Validado. |
| OpenLineage habilitado quando necessário. | Validado. |
| Custo de `OPTIMIZE` avaliado. | Validado. |

---

## 21. Contrato de primeira versão

Esta versão não mantém aliases legados de parâmetros ou modos de escrita. O contrato público deve ser usado exatamente como documentado.

Exemplos de nomes aceitos:

```python
watermark_columns
hash_exclude_columns
scd2_change_columns
scd2_historical
snapshot_soft_delete
```

Exemplos de nomes não aceitos:

```python
watermark_column
hash_exclude_cols
scd2_change_cols
scd2
snapshot
upsert
```

Parâmetros desconhecidos em `ingest()` geram erro. Essa decisão é intencional para evitar typos silenciosos em pipelines de produção.

---

## 22. Convenções recomendadas

### 22.1 Nome de tabelas

| Camada | Prefixo sugerido | Exemplo |
|---|---|---|
| Bronze | `b_` | `b_orders` |
| Silver | `c_` | `c_orders` |
| Gold | `gd_`, `dim_`, `fato_` | `dim_cliente`, `fato_vendas` |

### 22.2 Colunas técnicas

| Coluna | Descrição |
|---|---|
| `ingestion_date` | Data UTC da ingestão. |
| `source_system` | Sistema de origem. |
| `__run_id` | Execução que produziu o registro. |
| `row_hash` | Hash binário de comparação. |
| `valid_from` | Início da validade histórica. |
| `valid_to` | Fim da validade histórica. |
| `is_current` | Registro corrente no SCD2. |
| `is_active` | Registro ativo no snapshot. |
| `deleted_at` | Momento de marcação como ausente. |
| `changed_columns` | Colunas rastreadas como alteradas. |

### 22.3 Uso de separador em parâmetros string

Parâmetros que aceitam múltiplas colunas podem ser passados como lista Python ou string separada por `|`.

```python
merge_keys=["empresa", "filial", "cliente"]
```

ou:

```python
merge_keys="empresa|filial|cliente"
```

Para configuração declarativa em código Python, prefira listas. Para widgets e notebooks operacionais, strings com `|` são práticas.

---

## 23. Exemplo orientado a Databricks Workflows

```python
from lakehouse_ingestion import ingest

catalog = dbutils.widgets.get("catalog")
run_group_id = dbutils.widgets.get("run_group_id")
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")

result = ingest(
    source="b_nota_fiscal",
    target_table="c_nota_fiscal",
    catalog=catalog,
    layer="silver",
    mode="scd1_upsert",
    merge_keys="id_nota_fiscal",
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "required_columns": ["id_nota_fiscal", "updated_at"],
        "not_null": ["id_nota_fiscal"],
        "unique_key": ["id_nota_fiscal"]
    },
    on_quality_fail="fail",  # unique_key/required_columns são abort-only
    explain_mode=False,
    openlineage_enabled=True,
    lock_enabled=True,
    notebook_name="silver_nota_fiscal",
    run_group_id=run_group_id,
    master_job_id=job_id,
    master_run_id=run_id
)

if result["status"] != "SUCCESS":
    raise RuntimeError(result["error_message"])
```

---

## 24. Resumo executivo de uso

Para uma nova tabela, defina antes da implementação:

1. Camada alvo.
2. Modo de escrita.
3. Chave natural.
4. Watermark, se houver.
5. Regra de deduplicação.
6. Política de schema.
7. Regras mínimas de qualidade.
8. Necessidade de histórico.
9. Estratégia de concorrência.
10. Necessidade de explain e OpenLineage.

Esse conjunto forma o contrato mínimo de ingestão da tabela.
