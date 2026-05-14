# Changelog

Este projeto segue versionamento semântico enquanto a biblioteca evolui:

- `PATCH`: correção de bug sem mudança de contrato.
- `MINOR`: novo recurso compatível ou endurecimento planejado do contrato.
- `MAJOR`: mudança incompatível depois de adoção estável.

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

- Renomeia o produto/pacote distribuído para `contractforge`, mantendo o namespace Python `lakehouse_ingestion`.
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
