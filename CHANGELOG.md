# Changelog

Este projeto segue versionamento semântico enquanto a biblioteca evolui:

- `PATCH`: correção de bug sem mudança de contrato.
- `MINOR`: novo recurso compatível ou endurecimento planejado do contrato.
- `MAJOR`: mudança incompatível depois de adoção estável.

## 2.5.2 - 2026-05-16

- Corrige `shape.columns` para projetar todos os paths a partir do schema original do DataFrame.
- Evita falha quando um alias sobrescreve o nome de um struct pai antes de extrair campos irmãos, por exemplo `amount._VALUE -> amount` e `amount._currency -> currency`.
- Adiciona teste de regressão para projeção de campos aninhados irmãos com alias conflitante com o parent struct.

## 2.5.1 - 2026-05-16

- Corrige conectores incrementais para extrair o valor de watermarks tipados antes de montar predicates, parâmetros, headers ou bodies.
- Corrige falha real no JDBC incremental em segunda execução, onde o JSON completo do watermark era usado como literal SQL.
- Adiciona validação clara para watermark composto em `source.incremental` quando não há coluna incremental única.

## 2.5.0 - 2026-05-16

- Torna `layer` uma classificação lógica customizável, sem limitar o contrato a `bronze`, `silver` e `gold`.
- Mantém `target_schema` como schema físico explícito; quando omitido, `layer` segue como fallback do schema físico.
- Atualiza JSON Schema, CLI `contractforge init`, testes e documentação para aceitar layers como `stage`, `raw`, `trusted` e `curated`.
- Mantém a restrição operacional de Bronze apenas para o valor literal `layer: bronze`.
- Adiciona retry no registro de `ctrl_ingestion_metadata`, reduzindo falhas por concorrência no setup das control tables.

## 2.4.3 - 2026-05-15

- Adiciona suporte declarativo a Azure Blob com SAS no conector `azure_blob`.
- Permite `source.account_url`, `source.container` e `source.auth.sas_token`, montando automaticamente o path `wasbs://container@account.blob.core.windows.net/...`.
- Configura `fs.azure.sas.<container>.<account>.blob.core.windows.net` em tempo de execução quando `sas_token` é informado.
- Em Databricks serverless/Spark Connect, quando `spark.conf.set` é bloqueado, falha rápido com orientação para Unity Catalog External Location/Volume ou Network Policy/NCC; não há fallback REST implícito no `azure_blob`.
- Passa a aceitar `avro` e `xml` como formatos de arquivo em conectores de arquivo/object storage, delegando a leitura ao runtime Spark.
- Mantém segredos redigidos nos metadados de source e adiciona métricas de provider/container/auth configurada.

## 2.3.0 - 2026-05-15

- Ajusta `shape.columns` para atuar como projeção declarativa: quando declarado, só os aliases informados seguem como colunas de negócio.
- Remove automaticamente colunas técnicas gerenciadas pela ContractForge herdadas da origem antes de recriá-las na execução atual.
- Mantém a possibilidade de preservar uma coluna de origem com nome reservado via `column_mapping` para um nome não reservado.
- Melhora a composição bronze -> silver -> gold sem exigir `select_columns` apenas para limpar metadados técnicos da camada anterior.
- Atualiza testes e documentação da semântica de `shape` e colunas técnicas.

## 2.2.0 - 2026-05-15

- Adiciona `response.mode: raw` no conector `rest_api` para baixar payloads JSON complexos como string, uma linha por página.
- Mantém a estruturação de JSON aninhado no `shape.parse_json`, com schema DDL explícito, sem transformação semântica no conector.
- Adiciona `response.raw_column` para nomear a coluna de payload bruto e `response_page_number` para rastrear páginas.
- Adiciona limites `limits.max_page_bytes` e `limits.max_total_bytes` para proteger o driver contra payloads grandes.
- Registra métricas `response_mode`, `raw_payloads_read`, limites de bytes e bytes lidos em `source_metrics`.
- Atualiza documentação de REST API com exemplos de payload raw e recomendação de landing + Auto Loader para alto volume.

## 2.1.0 - 2026-05-15

- Adiciona conector nativo `http_file` para baixar arquivos HTTP(S) pelo driver Python e materializar DataFrame Spark sem depender de `spark.read` direto em `https://`.
- Adiciona aliases `http_csv`, `http_json` e `http_text`.
- Suporta `format=csv`, `json`, `jsonl`, `ndjson` e `text` em `http_file`.
- Adiciona validação estática de `source.path`/`source.request.url`, `source.format` e método HTTP GET para HTTP file.
- Registra métricas específicas em `source_metrics_json`: formato, registros lidos, bytes baixados, retry e `source_complete`.
- Atualiza documentação de conectores com exemplo de ingestão de CSV público via HTTP.

## 2.0.0 - 2026-05-15

- **Breaking:** renomeia o namespace Python para `contractforge`; imports antigos via `lakehouse_ingestion` foram removidos.
- **Breaking:** atualiza referências internas de observabilidade/lineage para o componente `contractforge`.
- Mantém o pacote distribuído e a CLI como `contractforge`.
- Adiciona `shape.zip_arrays` para transformar arrays paralelos em `array<struct>` antes de `shape.arrays`.
- Permite modelar respostas de APIs como Open-Meteo sem `arrays_zip`/`explode` manual em notebooks.
- Estende `shape.columns` com `cast` e `expression` para normalizações estruturais simples.
- Remove automaticamente aliases técnicos de `zip_arrays`/`explode` quando usados apenas como ponte para colunas finais.
- Atualiza JSON Schema, exports públicos, testes e documentação do `shape`.

## 1.16.0 - 2026-05-14

- Adiciona templates built-in de contratos para cenários REST, Auto Loader, JDBC/SCD1, snapshot soft delete, SCD2 e gold KPI.
- Adiciona CLI `contractforge templates list|show|write` para descobrir e gerar bundles YAML split.
- Expõe `list_contract_templates()`, `get_contract_template()`, `contract_template_details()` e `contract_template_files()` na API pública.
- Adiciona documentação de templates para acelerar onboarding e padronização de novos projetos.

## 1.15.0 - 2026-05-14

- Adiciona `contractforge maintenance ctrl-retention` para gerar ou aplicar limpeza das ctrl tables históricas.
- Expõe `build_ctrl_retention_plan()` e `apply_ctrl_retention()` na API pública.
- Mantém `ctrl_ingestion_state` e `ctrl_ingestion_metadata` fora da limpeza automática.
- Fortalece redaction de metadados de conectores, incluindo labels, paths e tabelas com padrões sensíveis.
- Adiciona testes de auditoria para garantir que metadados REST/JDBC não exponham credenciais.
- Adiciona documentação operacional de retenção, anti-patterns e exemplos YAML de JDBC/REST.

## 1.14.0 - 2026-05-14

- Separa a camada lógica (`layer`) do schema físico do target com o novo parâmetro `target_schema`.
- Mantém `layer` como default do schema físico quando `target_schema` não é informado.
- Aceita contratos no formato `target: {catalog, schema, table}` como alternativa declarativa a `catalog`/`target_schema`/`target_table`.
- Atualiza `contractforge init --target-schema` para gerar bundles split com annotations/operations/access apontando para o schema físico correto.
- Atualiza preview/governança, stream, ingestão e resolução de source não qualificado para usar o schema físico resolvido.

## 1.13.0 - 2026-05-14

- Adiciona `contractforge init` para gerar contratos YAML iniciais a partir da CLI.
- Suporta geração de contrato único ou bundle split com `.ingestion.yaml`, `.annotations.yaml`, `.operations.yaml` e `.access.yaml`.
- Valida chaves obrigatórias para modos que precisam de `merge_keys`/`hash_keys`.
- Atualiza documentação, site e template de projeto com o fluxo `init -> validate-project`.

## 1.12.0 - 2026-05-14

- Adiciona `contractforge validate-project` para descobrir e validar recursivamente contratos standalone e bundles split em uma árvore de projeto.
- Facilita uso em CI de projetos Databricks Asset Bundles sem listar arquivo por arquivo.
- Atualiza documentação e template de projeto com o novo fluxo de validação.

## 1.11.0 - 2026-05-14

- Adiciona `contractforge connectors doctor` para diagnosticar requisitos estáticos de conectores sem abrir SparkSession ou conexões externas.
- Expõe `diagnose_source_connectors()` na API pública.
- Documenta requisitos de runtime para Auto Loader, object storage, JDBC, Snowflake e BigQuery.
- Atualiza README, documentação oficial, guia de uso e site com o novo comando.

## 1.10.0 - 2026-05-14

- Adiciona aliases nativos de object storage: `s3`, `adls`, `azure_blob` e `gcs`, com provider inferido para observabilidade.
- Adiciona conectores de arquivo `delta` e `orc` por path usando `spark.read.format(...).load(path)`.
- Adiciona aliases JDBC nomeados: `postgres`, `postgresql`, `sqlserver`, `mysql` e `oracle`.
- Adiciona conectores Spark externos `snowflake` e `bigquery`, delegando para os conectores Spark instalados no runtime.
- Fortalece validação estática dos novos conectores em `contractforge validate`.
- Atualiza README, documentação oficial e site com exemplos YAML dos novos conectores.

## 1.9.0 - 2026-05-14

- Adiciona camada declarativa de conectores de source com `ConnectorSpec` e registry `register_source_resolver`.
- Inclui conectores nativos para tabela/view, SQL, arquivos (`parquet`, `json`, `csv`, `text`), object storage/blob (`adls`, `azure_blob`, `s3`, `gcs`), JDBC e REST API.
- Permite conectores customizados em YAML/JSON usando qualquer `source.connector` com nome válido, desde que um resolver seja registrado no runtime.
- Adiciona CLI `contractforge connectors list|show` e validação estática dos campos obrigatórios dos conectores nativos em `contractforge validate`.
- Suporta REST API batch com autenticação `bearer_token`, `api_key`, `basic` e `oauth_client_credentials`, paginação `page`, `offset`, `cursor` e `link_header`, retry/backoff, timeout e rate limit simples.
- Suporta pushdown incremental em REST (`watermark_param`, `watermark_header`, `watermark_body_field`) e JDBC (`watermark_column`/`predicate`) usando o watermark anterior registrado pela lib.
- Permite Auto Loader também no formato unificado `source.type=connector` com `connector=autoloader`.
- Registra metadados redigidos de source em `ctrl_ingestion_runs` (`source_connector`, provider, formato, path, opções, request/auth/pagination/incremental/limits e capabilities).
- Registra observabilidade específica de conectores em `ctrl_ingestion_runs.source_metrics_json`, incluindo requests/páginas/bytes/registros para REST e estratégia/incrementalidade/particionamento para JDBC.
- Permite declarar `source.read.source_complete=true` ou `full_snapshot=true` em conectores para modos que exigem snapshot completo.
- Atualiza JSON Schema, exports públicos e documentação com exemplos YAML de conectores.
- Eleva `ctrl_schema_version` para 11.

## 1.8.1 - 2026-05-13

- Renomeia o produto/pacote distribuído para `contractforge`.
- Renomeia a CLI para `contractforge`.
- Move `pyspark` e `delta-spark` para o extra opcional `spark`, evitando que wheels instalados em Databricks/serverless tentem resolver dependências já fornecidas pelo runtime.
- Mantém o extra `dev` com Spark/Delta para testes locais completos e CI.

## 1.8.0 - 2026-05-13

- Adiciona `shape` para transformar estruturas JSON/struct/array antes de quality/write.
- Suporta flatten recursivo de structs, extração de paths aninhados com alias e arrays em modos `keep`, `to_json`, `size`, `first`, `explode` e `explode_outer`.
- Bloqueia mudança de cardinalidade em Bronze por padrão, exigindo `shape.allow_cardinality_change_on_bronze=true`.
- Detecta múltiplos explodes irmãos que poderiam gerar produto cartesiano, exigindo `allow_cartesian=true`.
- Adiciona exemplos YAML de flatten e arrays aninhados na documentação.

## 1.7.0 - 2026-05-13

- Adiciona presets declarativos para padrões comuns de ingestão Bronze/Silver/Gold.
- Expõe `apply_preset`, `list_presets`, `get_preset`, `preset_details` e `register_preset`.
- Adiciona CLI `contractforge presets list|show` e `validate --expand-presets`.
- Registra `applied_presets` no plano e no retorno das execuções para auditoria.
- Adiciona modificadores reutilizáveis de quality, Delta properties, runtime e governança.

## 1.6.4 - 2026-05-13

- Define semântica explícita para `access_policy.on_drift`.
- `on_drift=fail` agora falha antes de aplicar grants quando há drift.
- `validate-access` e `governance-check` retornam `FAILED` para drift com `on_drift=fail` e `WARNED` para drift tolerado.
- Issues de drift passam a refletir severidade `fail` ou `warn` conforme a política.

## 1.6.3 - 2026-05-13

- Faz `governance-apply` aplicar somente `operations` e `annotations`, mantendo `access` exclusivo do comando dedicado.
- Adiciona API `apply_annotations_bundle()` e CLI `contractforge apply-annotations`.
- Adiciona CLI `contractforge validate-access` para validar contrato de acesso e drift sem aplicar mudanças.
- Adiciona `annotations_preview` estruturado no retorno de `dry_run`.

## 1.6.2 - 2026-05-13

- Separa o ciclo de access da ingestão normal: `ingest_plan` aplica `operations`/`annotations` e deixa `access` como `DEFERRED`.
- Adiciona API `apply_access_bundle()` e CLI `contractforge apply-access`.
- Adiciona alias CLI `contractforge drift-check`.
- Adiciona validação conservadora de capabilities Unity Catalog para tags, row filters e column masks.

## 1.6.1 - 2026-05-13

- Aceita o formato separado com `target`, `operations`/`ownership`, `access_policy` e `column_masks` como mapa por coluna.
- Bloqueia `revoke_unmanaged=true` sem confirmação explícita no comando dedicado de access.
- Fortalece validações de governança: `expected_frequency`, privilégios UC, funções qualificadas, descrições vazias, aliases vazios e `deprecated` incompleto.
- Expande auditoria em `ctrl_ingestion_annotations`, `ctrl_ingestion_operations`, `ctrl_ingestion_access` e resumo de governança em `ctrl_ingestion_runs`.
- Eleva `ctrl_schema_version` para 9.

## 1.6.0 - 2026-05-13

- Adiciona contratos declarativos separados para `annotations`, `operations` e `access`.
- Aplica comments/tags de tabela e coluna, incluindo aliases, PII e depreciação, com auditoria em `ctrl_ingestion_annotations`.
- Registra contrato operacional em `ctrl_ingestion_operations` para dashboards e alertas externos.
- Aplica grants, row filters e column masks declarativos com auditoria em `ctrl_ingestion_access`.
- Adiciona loader de bundle (`load_contract_bundle`) e CLI `contractforge validate-bundle`.
- Adiciona `_metadata` por arquivo de contrato e preview de governança (`governance_preview`).
- Adiciona aplicação assíncrona de governança (`apply_governance_bundle`) e CLI `governance-preview`/`governance-apply`.
- Adiciona validação de governança contra schema real do target (`validate_governance_contract`) e CLI `governance-check`.
- Adiciona relatório de drift de grants (`access_drift_report`), preenchimento de `previous_value` e suporte a `revoke_unmanaged=true`.
- Eleva `ctrl_schema_version` para 8.

## 1.5.1 - 2026-05-13

- Corrige métricas agregadas de streams Autoloader/`SourceSpec` quando `foreachBatch` registra batches filhos, mas o estado local do driver não reflete os resultados em Spark Connect/serverless.
- Normaliza métricas de micro-batches entre `rows_*` e `total_rows_*` antes de consolidar `ctrl_ingestion_streams`.

## 1.5.0 - 2026-05-12

- Adiciona `SourceSpec` declarativo para Autoloader em modo `available_now`.
- Adiciona registry de source resolvers (`register_source_resolver`).
- Adiciona `ingest_stream_plan` com `foreachBatch` reaproveitando `ingest_plan` por batch.
- Adiciona `ctrl_ingestion_streams` e eleva `ctrl_schema_version` para 7.
- Aplica idempotência no nível do stream e por batch para evitar duplicação em reexecuções.

## 1.4.0 - 2026-05-12

- Adiciona `column_mapping` para renomear source -> target com validação de colisões e colunas técnicas.
- Adiciona `delta_properties` para aplicar TBLPROPERTIES na criação de tabelas Delta.
- Permite `retry_attempts` e `retry_backoff_seconds` por plano.
- Bloqueia sobrescrita silenciosa de colunas técnicas vindas da origem.
- Valida `merge_keys` totalmente nulas antes de executar `MERGE` e alerta para nulos parciais.
- Otimiza `quality_rules.expressions` para entrar na agregação single-pass de quality.
- Adiciona `IngestionHooks`, `register_write_mode`, `yaml_schema()` e CLI `contractforge validate/schema`.

## 1.3.1 - 2026-05-11

- Adiciona workflow de CI para lint, testes puros e validação de build.
- Adiciona `scripts/check_release.py` para garantir sincronismo de versão, changelog e metadados do pacote.
- Expõe URLs do projeto no metadata do wheel.

## 1.3.0 - 2026-05-11

- Endurece parsing de `quality_rules` com rejeição de campos desconhecidos, thresholds inválidos e expressões malformadas.
- Normaliza listas declarativas em regras de qualidade vindas de YAML/notebook.
- Valida cedo `runtime_parameters`, `tags`, `idempotency_policy` e combinação `allow_type_widening` com `schema_policy`.
- Atualiza metadados de pacote para licença SPDX e adiciona `LICENSE`.

## 1.2.0 - 2026-05-11

- Adiciona `allow_type_widening` para alargamentos seguros de tipo.
- Registra evolução estrutural em `ctrl_ingestion_schema_changes`.
- Expõe `stage_durations` e `contract_metadata` no retorno da ingestão.
- Propaga metadados declarativos para `ctrl_ingestion_runs`.

## 1.1.0 - 2026-05-11

- Formaliza severidade e mensagem em `quality_rules.expressions`.
- Padroniza observabilidade de erros, runtime e métricas lógicas.
- Evolui idempotência operacional e locks best-effort.

## 1.0.2 - 2026-05-11

- Corrige compatibilidade de deduplicação com Spark Connect/serverless.
- Ajusta SCD hash diff e SCD2 para evitar referências ambíguas após join.

## 1.0.0 - 2026-05-11

- Primeira linha funcional validada com harness Databricks e fluxo Medallion.
