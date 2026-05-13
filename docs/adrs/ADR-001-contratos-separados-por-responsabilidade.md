# ADR-001: Contratos separados por responsabilidade

**Status:** Aceita
**Data:** 2026-05-13

## Contexto

O contrato de uma tabela cobre responsabilidades diferentes: engenharia de dados, governanca de catalogo, operacao/SRE e seguranca. Em empresas maiores, esses temas possuem ciclos de revisao, aprovadores e riscos diferentes. Manter tudo em um unico YAML aumenta conflito de merge, dificulta revisao e acopla mudancas que poderiam ser aplicadas de forma independente.

## Decisao

O framework suporta contratos separados por responsabilidade, carregados como bundle:

- `*.ingestion.yaml`: fonte, destino, modo de escrita, schema policy, qualidade, watermark, particionamento e parametros de execucao.
- `*.annotations.yaml`: comentarios, aliases, tags, PII, deprecacao e metadata de tabela/coluna.
- `*.operations.yaml`: ownership, criticidade, SLA, grupos, runbook e parametros para dashboards operacionais.
- `*.access.yaml`: grants, row filters, column masks e politica de drift/reconcile.

`load_contract_bundle()` e `ingest_bundle()` unem esses arquivos quando a ingestao precisa do contexto completo. Comandos dedicados permitem validar ou aplicar partes especificas sem reexecutar a ingestao pesada.

## Consequencias

- Times diferentes conseguem revisar arquivos diferentes sem bloquear todo o contrato.
- Metadata, operacao e acesso podem evoluir sem alterar a logica de ingestao.
- Fica mais simples detectar drift por dimensao: ingestao, anotacao, operacao ou acesso.
- O custo e a complexidade de descoberta de contrato aumentam: a documentacao e a CLI precisam deixar claro quais arquivos fazem parte do bundle.
- Contratos com muitos arquivos exigem padrao forte de nomenclatura e versionamento.
