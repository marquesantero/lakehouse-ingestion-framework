# ADR-003: `snapshot_soft_delete` via SQL MERGE

**Status:** Aceita
**Data:** 2026-05-13

## Contexto

`snapshot_soft_delete` tem uma semantica diferente de uma carga incremental. A fonte representa o estado final completo da entidade no momento da execucao. Qualquer linha existente no target e ausente na fonte deve ser marcada como inativa (`is_active=false`, `deleted_at=now()`).

Se a fonte for parcial, por exemplo por `watermark_columns`, `filter_expression` ou Autoloader incremental, o framework nao consegue distinguir "registro removido da origem" de "registro apenas fora do recorte carregado". Isso geraria soft deletes falsos.

Tambem houve divergencia entre runtimes quando a implementacao dependia de APIs Python da DeltaTable. Databricks Serverless/Spark Connect tem limites diferentes de clusters classicos.

## Decisao

O modo `snapshot_soft_delete`:

- exige source completo do estado atual;
- rejeita `watermark_columns`, `filter_expression` e `SourceSpec` declarativo;
- usa SQL `MERGE` em todos os runtimes, incluindo classic e serverless;
- usa `WHEN NOT MATCHED BY SOURCE` para marcar linhas ativas ausentes como inativas;
- reativa linhas que voltam a aparecer no snapshot;
- calcula `row_hash` para atualizar apenas registros alterados.

## Consequencias

- A semantica do modo fica previsivel: snapshot completo entra, estado final consistente sai.
- O mesmo caminho SQL reduz divergencia entre classic, Serverless e Spark Connect.
- A API falha cedo para combinacoes conceitualmente incorretas.
- Pode haver custo maior que um caminho incremental, porque o source precisa representar o conjunto completo.
- Para carga incremental, o contrato deve usar `scd1_upsert`, `scd1_hash_diff` ou outro modo adequado.
