# ADR-002: Access control deferred

**Status:** Aceita
**Data:** 2026-05-13

## Contexto

Aplicar grants, row filters e column masks em Unity Catalog normalmente exige permissoes diferentes das permissoes usadas para escrever dados. Se a ingestao normal tentasse aplicar acesso diretamente, o job de dados precisaria receber privilegios elevados e uma falha de governanca poderia mascarar o resultado da carga.

Tambem existe risco operacional em reconciliacao de grants. Revogar permissao nao declarada pode remover acesso manual criado para emergencia, investigacao ou transicao.

## Decisao

`ingest_plan()` aplica `operations` e `annotations` depois da escrita, mas deixa `access` como `DEFERRED`. A aplicacao de acesso fica em comandos dedicados:

- `lakehouse-ingest validate-access`
- `lakehouse-ingest governance-check`
- `lakehouse-ingest drift-check`
- `lakehouse-ingest apply-access`

Revogacoes de grants nao declarados exigem `revoke_unmanaged=true` no contrato e `apply-access --force-revoke` na execucao.

## Consequencias

- O job de ingestao nao precisa de privilegios elevados de seguranca.
- A esteira de seguranca pode rodar com credenciais, aprovadores e auditoria proprios.
- Falhas de access nao interrompem carga de dados por padrao; elas aparecem como status/relatorio de governanca.
- O rollout passa a ter duas etapas quando access precisa ser aplicado: ingestao e governanca.
- O harness deve testar access separadamente da execucao principal da tabela.
