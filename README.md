<p align="center">
  <img src="docs/assets/logo/contractforge-logo.png" alt="ContractForge" width="520">
</p>

# ContractForge

ContractForge é um framework **contract-first** para ingestão governada em Delta Lake e Databricks. Em vez de espalhar lógica de ingestão, schema, qualidade, observabilidade e governança em notebooks ad-hoc, você descreve a intenção em contratos versionáveis e a biblioteca executa o padrão operacional.

Links principais:

- **Documentação web:** https://marquesantero.github.io/contractforge/
- **Guia rápido:** [docs/quickstart.md](docs/quickstart.md)
- **Mapa da documentação:** [docs/README.md](docs/README.md)
- **Documentação completa:** [docs/oficial.md](docs/oficial.md)
- **Template de projeto:** [examples/project_template](examples/project_template)
- **Playground de exemplos:** [examples/playground](examples/playground)
- **Changelog e releases:** [CHANGELOG.md](CHANGELOG.md)
- **Contribuição:** [CONTRIBUTING.md](CONTRIBUTING.md)
- **Segurança:** [SECURITY.md](SECURITY.md)

## O Que Ele Resolve

- Padroniza ingestões por classificação lógica (`bronze`, `silver`, `gold`, `stage`, `raw`, `curated` etc.) com contratos YAML ou chamadas Python.
- Separa `layer` lógico do schema físico com `target_schema`, permitindo organizações como `main.crm_curated.c_cliente` sem obrigar schema por camada.
- Suporta modos oficiais de escrita: append, overwrite, SCD1, hash-diff, SCD2 e snapshot com soft delete.
- Aplica quality gates, quarentena, schema policy, watermarks, idempotência, locks e retry.
- Registra observabilidade em ctrl tables: runs, erros, qualidade, quarentena, lineage, streaming, schema changes, annotations, operations e access.
- Integra governança declarativa com `*.annotations.yaml`, `*.operations.yaml` e `*.access.yaml`.
- Resolve fontes declarativas via conectores: tabelas, SQL, arquivos, HTTP files, object storage, JDBC, REST API, Auto Loader `available_now`, Snowflake e BigQuery.
- Protege MERGE contra `merge_keys` duplicadas na source e redige secrets também em tracebacks/erros persistidos.

## Posicionamento

ContractForge não tenta substituir Delta Live Tables/Lakeflow. O objetivo é oferecer controle fino, contratos revisáveis por tabela e portabilidade para jobs, notebooks, Databricks Asset Bundles e runtimes Spark/Delta compatíveis.

Em Databricks, ele complementa Unity Catalog aplicando comments/tags e gerando evidências operacionais em tabelas Delta de controle.

## Instalação

O pacote distribuído e o namespace Python se chamam `contractforge`.

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
from contractforge import ingest

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
  auth:
    type: basic
    username: "{{ secret:erp/user }}"
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

`layer` é metadata operacional e pode ser customizado. O schema físico é `target.schema` ou `target_schema`; se omitido, a lib usa `layer` como fallback.

Para Amazon RDS/Aurora, `connector: postgres` também aceita `auth.type: rds_iam`, gerando token IAM no driver Python com credenciais explícitas, variáveis `AWS_*` ou `credential_provider: default_chain` (`contractforge[aws]`). A conectividade de rede continua responsabilidade do runtime: mesma VPC, VPC peering, Transit Gateway, PrivateLink/NLB, endpoint público tradicional ou Aurora Express Internet Access Gateway. Veja o guia completo em [docs/rds_iam_jdbc.md](docs/rds_iam_jdbc.md).

## Transformações Declarativas

`transform` é o namespace canônico para mudanças físicas antes de quality/write. Use `transform.shape` para normalizar JSON, structs e arrays, e `transform.deduplicate` para reduzir múltiplas versões por chave antes de MERGE. O campo histórico `shape` continua aceito como atalho, mas novos contratos devem usar `transform.shape`.

Quando `transform.shape.columns` é declarado, ele funciona como projeção: só os aliases declarados seguem como colunas de negócio, evitando carregar campos brutos ou colunas técnicas de camadas anteriores. Para arrays paralelos de APIs, use `zip_arrays` antes do `explode`:

```yaml
transform:
  shape:
    zip_arrays:
      - alias: hourly_rows
        columns:
          hourly.time: time
          hourly.temperature_2m: temperature_2m
    arrays:
      - path: hourly_rows
        mode: explode_outer
        alias: hour
    columns:
      location_id: location_id
      hour.time: forecast_hour
      hour.temperature_2m:
        alias: temperature_2m
        cast: DOUBLE
      forecast_date:
        expression: "TO_DATE(hour.time)"
        alias: forecast_date
```

Deduplicação declarativa evita MERGE ambíguo quando a fonte traz múltiplas versões da mesma chave:

```yaml
transform:
  deduplicate:
    keys: [order_id]
    order_by: "updated_at DESC NULLS LAST, ingestion_sequence DESC"
```

## CLI

```bash
contractforge init --output contracts/silver/s_orders --source raw.orders --target-table s_orders --layer silver --target-schema sales_curated --mode scd1_upsert --merge-keys order_id --split
contractforge validate-bundle contracts/silver/s_orders
contractforge validate-project contracts
contractforge templates list
contractforge templates write silver_jdbc_scd1_upsert --output contracts/silver/s_orders
contractforge presets list
contractforge connectors doctor postgres rest_api http_file s3
contractforge maintenance ctrl-retention --catalog main --ctrl-schema ops --retention-days 180
python examples/playground/scripts/validate_playground.py
```

## HTTP File

Para arquivos públicos ou autenticados via HTTP(S), use `http_file` em vez de depender de `spark.read` direto em `https://`. Isso evita limitações de filesystem do Spark/serverless e mantém o arquivo como fonte declarativa.

```yaml
source:
  type: connector
  connector: http_file
  path: https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv
  format: csv
  options:
    header: true
  read:
    source_complete: true

target:
  catalog: workspace
  schema: cf_examples_bronze
  table: b_covid_brazil_states

layer: bronze
mode: scd0_overwrite
```

Formatos suportados: `csv`, `json`, `jsonl`, `ndjson` e `text`. Aliases: `http_csv`, `http_json` e `http_text`.

## Azure Blob

Em Databricks serverless, prefira Unity Catalog External Location/Volume e leia o path governado diretamente. Esse é o caminho mais previsível para Azure Blob/ADLS, porque credencial e rede ficam sob governança do Unity Catalog.

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

Em job cluster/classic/local, também é possível declarar SAS com `account_url`, `container` e `auth.sas_token`; nesse caso a ContractForge monta `wasbs://...` e configura `fs.azure.sas...` no Spark. Em Databricks serverless/Spark Connect, essa configuração pode ser bloqueada; nesse caso a ContractForge falha rápido com orientação para usar Unity Catalog External Location/Volume (`abfss://...` ou `/Volumes/...`) ou configurar Serverless Network Policy/NCC para liberar o destino. O conector `azure_blob` não faz fallback REST implícito, porque isso muda semântica, custo, limites de memória e comportamento de rede. Para arquivos HTTP(S) explícitos de volume controlado, use `http_file`.

## S3

Em Databricks serverless, prefira Unity Catalog External Location/Volume e leia `s3://...` governado diretamente. Em classic/job cluster/local, a ContractForge também pode configurar S3A a partir de `source.auth`:

```yaml
source:
  type: connector
  connector: s3
  path: s3a://company-landing/orders/
  format: csv
  auth:
    access_key_id: "{{ secret:aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:aws/aws_secret_access_key }}"
    session_token: "{{ secret:aws/aws_session_token }}" # opcional
  options:
    header: true
    fs.s3a.endpoint: s3.us-east-1.amazonaws.com
  read:
    source_complete: true
    schema: "order_id STRING, customer_id STRING, amount DOUBLE"
```

Com `session_token`, a lib usa `TemporaryAWSCredentialsProvider`; sem ele, usa `SimpleAWSCredentialsProvider`. Se o runtime bloquear `spark.conf.set` para `fs.s3a.*`, a execução falha com orientação para usar External Location/Volume.

Formatos de arquivo aceitos em file/object storage: `avro`, `csv`, `delta`, `json`, `jsonl`, `ndjson`, `orc`, `parquet`, `text` e `xml`. `jsonl/ndjson` são mapeados para o reader Spark `json`. `avro/xml/parquet/orc/delta` dependem do reader Spark e de acesso configurado no runtime/Unity Catalog. Excel não é formato Spark nativo; use um conector Spark específico quando necessário.

Quando o schema é conhecido, use `source.read.schema` com DDL Spark. `source.schema` também é aceito como alias curto e é normalizado para `source.read.schema`; se ambos forem declarados com valores diferentes, o contrato falha antes da leitura. Isso evita inferência em diretórios grandes ou com muitos arquivos pequenos e é registrado em `source_metrics_json.schema_declared`.

Para APIs REST com JSON complexo, use `response.mode: raw` e deixe `transform.shape` estruturar o payload com schema explícito:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://eonet.gsfc.nasa.gov/api/v3/events
    params:
      status: open
  response:
    mode: raw
    raw_column: raw_response
  limits:
    max_page_bytes: 10485760
    max_total_bytes: 52428800

transform:
  shape:
    parse_json:
      - column: raw_response
        alias: payload
        schema: "STRUCT<events: ARRAY<STRUCT<id: STRING, title: STRING>>>"
```

O conector continua responsável só por baixar e proteger o volume; `transform.shape` faz parse/flatten/explode, e `annotations` governa catálogo, tags e PII.

Quando a API já retorna uma lista de registros, `response.records_path` suporta navegação simples (`$`, `$.data.items`, `$[1]`, `$.data[0].items`) em `rest_api` e `http_file` JSON. Não é JSONPath completo; para payloads complexos, prefira `response.mode: raw` + `transform.shape`.

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

Guias operacionais úteis:

- [Operação e manutenção](docs/operacao.md)
- [Templates de contratos](docs/templates.md)
- [Compatibilidade de conectores](docs/compatibilidade_conectores.md)
- [Segurança e secrets](docs/seguranca.md)
- [Anti-patterns](docs/antipadroes.md)

## Desenvolvimento

```bash
pip install -e ".[dev]"
pytest
python scripts/check_release.py
```

Antes de abrir PR, leia [CONTRIBUTING.md](CONTRIBUTING.md). A branch `main` é protegida e exige PR, revisão, resolução de conversas e os checks `build`, `test (3.10)` e `test (3.11)`.

Release:

```bash
python -m build
twine check dist/*
git tag vX.Y.Z
git push origin vX.Y.Z
```

O workflow `Release` valida metadados, confere se a tag bate com a versão do pacote, gera wheel/source distribution e anexa os artefatos à GitHub Release.

## Licença

MIT. Consulte [LICENSE](LICENSE).
