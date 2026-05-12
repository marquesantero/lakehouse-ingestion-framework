# Changelog

Este projeto segue versionamento semântico enquanto a biblioteca evolui:

- `PATCH`: correção de bug sem mudança de contrato.
- `MINOR`: novo recurso compatível ou endurecimento planejado do contrato.
- `MAJOR`: mudança incompatível depois de adoção estável.

## 1.4.0 - 2026-05-12

- Adiciona `column_mapping` para renomear source -> target com validação de colisões e colunas técnicas.
- Adiciona `delta_properties` para aplicar TBLPROPERTIES na criação de tabelas Delta.
- Permite `retry_attempts` e `retry_backoff_seconds` por plano.
- Bloqueia sobrescrita silenciosa de colunas técnicas vindas da origem.
- Valida `merge_keys` totalmente nulas antes de executar `MERGE` e alerta para nulos parciais.
- Otimiza `quality_rules.expressions` para entrar na agregação single-pass de quality.
- Adiciona `IngestionHooks`, `register_write_mode`, `yaml_schema()` e CLI `lakehouse-ingest validate/schema`.

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
