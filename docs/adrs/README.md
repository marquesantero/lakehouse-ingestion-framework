# Architecture Decision Records

Este diretório registra decisões arquiteturais que explicam escolhas de produto e engenharia do ContractForge.

Formato usado:

- **Status:** proposta, aceita, substituida ou removida.
- **Contexto:** problema que motivou a decisao.
- **Decisao:** escolha adotada.
- **Consequencias:** efeitos positivos, custos e restricoes.

## ADRs

| ADR | Status | Decisao |
|-----|--------|---------|
| [ADR-001](ADR-001-contratos-separados-por-responsabilidade.md) | Aceita | Separar contratos de ingestion, annotations, operations e access. |
| [ADR-002](ADR-002-access-deferred.md) | Aceita | Nao aplicar governanca de acesso dentro da ingestao normal. |
| [ADR-003](ADR-003-snapshot-soft-delete-sql-merge.md) | Aceita | Implementar `snapshot_soft_delete` com SQL `MERGE` e bloquear fontes parciais. |
