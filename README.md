<p align="center">
  <img src="site/assets/logo/contractforge-logo.png" alt="ContractForge" width="520">
</p>

# ContractForge

ContractForge é um framework **contract-first** para ingestão governada em Delta Lake e Databricks. Em vez de espalhar lógica de ingestão, schema, qualidade, observabilidade e governança em notebooks ad-hoc, você descreve a intenção em contratos versionáveis e a biblioteca executa o padrão operacional.

Links principais:

- **Documentação web:** https://marquesantero.github.io/contractforge/
- **Guia rápido:** [docs/quickstart.md](docs/quickstart.md)
- **Mapa da documentação:** [docs/README.md](docs/README.md)
- **Documentação completa:** [docs/oficial.md](docs/oficial.md)
- **Template de projeto:** [examples/project_template](examples/project_template)
- **Changelog e releases:** [CHANGELOG.md](CHANGELOG.md)

## O Que Ele Resolve

- Padroniza ingestões Bronze/Silver/Gold com contratos YAML ou chamadas Python.
- Separa `layer` lógico do schema físico com `target_schema`, permitindo organizações como `main.crm_curated.c_cliente`.
- Suporta modos oficiais de escrita: append, overwrite, SCD1, hash-diff, SCD2 e snapshot com soft delete.
- Aplica quality gates, quarentena, schema policy, watermarks, idempotência, locks e retry.
- Registra observabilidade em ctrl tables: runs, erros, qualidade, quarentena, lineage, streaming, schema changes, annotations, operations e access.
- Integra governança declarativa com `*.annotations.yaml`, `*.operations.yaml` e `*.access.yaml`.
- Resolve fontes declarativas via conectores: tabelas, SQL, arquivos, object storage, JDBC, REST API, Auto Loader `available_now`, Snowflake e BigQuery.

## Posicionamento

ContractForge não tenta substituir Delta Live Tables/Lakeflow. O objetivo é oferecer controle fino, contratos revisáveis por tabela e portabilidade para jobs, notebooks, Databricks Asset Bundles e runtimes Spark/Delta compatíveis.

Em Databricks, ele complementa Unity Catalog aplicando comments/tags e gerando evidências operacionais em tabelas Delta de controle.

## Instalação

O pacote distribuído se chama `contractforge`. O namespace Python permanece `lakehouse_ingestion`.

```bash
pip install contractforge
```

Para desenvolvimento local a partir do repositório:

```bash
pip install -e ".[dev]"
```

Para executar Spark/Delta fora do Databricks:

```bash
pip install ".[spark]"
```

No Databricks, o wheel não declara `pyspark` nem `delta-spark` como dependências obrigatórias, porque o runtime já fornece Spark e Delta.

## Exemplo Python

```python
from lakehouse_ingestion import ingest

result = ingest(
    source=df,
    target_table="s_orders",
    catalog="main",
    layer="silver",
    target_schema="sales_curated",
    mode="scd1_upsert",
    merge_keys="order_id",
    column_mapping={"id": "order_id"},
    watermark_columns="updated_at",
    dedup_order_expr="updated_at DESC NULLS LAST",
    schema_policy="additive_only",
    quality_rules={
        "not_null": ["order_id"],
        "unique_key": ["order_id"],
    },
)
```

## Exemplo YAML

```yaml
preset: silver_scd1_upsert

source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    dbtable: public.orders
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"

target:
  catalog: main
  schema: sales_curated
  table: s_orders

layer: silver
merge_keys: order_id
watermark_columns: updated_at
schema_policy: additive_only

quality_rules:
  not_null: [order_id]
  unique_key: [order_id]
```

## CLI

```bash
contractforge init --output contracts/silver/s_orders --source raw.orders --target-table s_orders --layer silver --target-schema sales_curated --mode scd1_upsert --merge-keys order_id --split
contractforge validate-bundle contracts/silver/s_orders
contractforge validate-project contracts
contractforge presets list
contractforge connectors doctor postgres rest_api s3
```

## Contratos Separados

Contratos podem ser mantidos em arquivos separados quando engenharia, governança, operações e segurança têm ciclos de revisão diferentes:

```text
contracts/gold/gd_orders.ingestion.yaml
contracts/gold/gd_orders.annotations.yaml
contracts/gold/gd_orders.operations.yaml
contracts/gold/gd_orders.access.yaml
```

O arquivo `*.ingestion.yaml` define a execução. `annotations` documenta tabela/colunas, tags, aliases e PII. `operations` registra dono, criticidade, SLA, grupos e runbook. `access` declara grants, row filters e column masks.

## Documentação

Comece pelo [guia rápido](docs/quickstart.md). Para navegação completa por tema, use [docs/README.md](docs/README.md). A documentação web publicada em GitHub Pages fica em https://marquesantero.github.io/contractforge/.

## Desenvolvimento

```bash
pip install -e ".[dev]"
pytest
python scripts/check_release.py
```

Release:

```bash
python -m build
twine check dist/*
git tag v1.14.0
git push origin v1.14.0
```

O workflow `Release` valida metadados, confere se a tag bate com a versão do pacote, gera wheel/source distribution e anexa os artefatos à GitHub Release.

## Licença

MIT. Consulte [LICENSE](LICENSE).
