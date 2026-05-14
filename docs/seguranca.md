# Segurança e Secrets

Este guia resume onde dados sensíveis podem aparecer e quais práticas devem ser usadas ao operar o ContractForge.

## Secrets

Use placeholders:

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:erp/postgres_url }}"
    user: "{{ secret:erp/user }}"
    password: "{{ secret:erp/password }}"
```

Resolução:

- Primeiro tenta variável de ambiente `CONTRACTFORGE_SECRET_<SCOPE>_<KEY>`.
- Se não existir, tenta Databricks Secrets via `dbutils.secrets.get(scope, key)`.
- Estruturas gravadas em logs/ctrl tables são redigidas antes de persistir.

## Campos redigidos

São tratados como sensíveis quando a chave contém termos como:

- `authorization`
- `password`
- `secret`
- `token`
- `api_key`
- `apikey`
- `key`

Também são redigidos valores no formato `{{ secret:scope/key }}`.

## Explain e lineage

- `explain_mode` deve ser usado para diagnóstico, não como logging permanente em produção.
- Evite colocar SQL com literais sensíveis em `source.query`, `filter_expression`, `dedup_order_expr` ou quality expressions.
- Eventos OpenLineage devem carregar metadados operacionais, não credenciais ou payloads de negócio.
- Se um conector externo exigir opções sensíveis com nomes não padronizados, prefira nomes contendo `secret`, `token`, `password` ou `key` para garantir redação automática.

## Ctrl tables

Restrinja acesso ao schema `ops`:

- `ctrl_ingestion_runs` contém nomes de fontes, targets, parâmetros redigidos e mensagens de erro.
- `ctrl_ingestion_errors` pode conter stack trace completo.
- `ctrl_ingestion_quarantine` pode conter payloads rejeitados e deve seguir a mesma política de acesso dos dados de origem.
- `ctrl_ingestion_lineage` pode revelar topologia de dados.

## Checklist

- Use Databricks Secrets ou variáveis de ambiente, nunca segredo literal em YAML.
- Revise `source.query` e expressões para evitar literais sensíveis.
- Aplique grants restritos no schema `ops`.
- Trate quarentena como dado sensível.
- Use `annotations.columns.<col>.pii` para marcar PII e facilitar auditoria.

