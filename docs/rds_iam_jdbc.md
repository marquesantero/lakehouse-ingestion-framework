# Amazon RDS/Aurora JDBC com IAM Auth

Este guia descreve o caminho validado para ler Amazon RDS/Aurora via JDBC usando `source.auth.type: rds_iam`.

Validação real executada:

- Runtime: Azure Databricks classic single-node.
- Cluster: `SINGLE_USER`, com driver PostgreSQL JDBC instalado.
- Banco: Amazon Aurora PostgreSQL 17.7.
- ContractForge: `2.6.5` e superior.
- Resultado: `ingest()` com `connector: postgres`, `auth.type: rds_iam`, particionamento JDBC e `scd1_hash_diff` terminou `SUCCESS`.

## Quando usar

Use `auth.type: rds_iam` quando a fonte for Amazon RDS/Aurora com IAM database authentication habilitado e você quiser evitar senha fixa de banco no contrato.

Use `auth.type: basic` apenas quando o banco aceitar autenticação por usuário/senha tradicional:

```yaml
source:
  auth:
    type: basic
    username: "{{ secret:scope/db_user }}"
    password: "{{ secret:scope/db_password }}"
```

## Pré-requisitos

- O endpoint RDS/Aurora precisa estar acessível por TCP a partir do compute Databricks.
- O driver JDBC do banco precisa estar instalado no cluster.
- O usuário do banco precisa existir e estar autorizado para IAM auth.
- A IAM principal usada pela ContractForge precisa ter `rds-db:connect`.
- As credenciais AWS precisam estar em `source.auth`, Databricks Secrets, variáveis de ambiente ou na AWS credential provider chain quando `credential_provider: default_chain` for usado.

## Driver JDBC no Databricks

Para PostgreSQL:

```text
org.postgresql:postgresql:42.7.4
```

Em clusters Unity Catalog `standard`/shared, Maven libraries podem exigir artifact allowlist. Se a instalação falhar com mensagem de allowlist, há duas opções:

- Pedir ao admin do metastore para allowlistar o artefato Maven.
- Usar cluster `SINGLE_USER` para validações controladas.

## Usuário PostgreSQL

Exemplo com usuário dedicado:

```sql
CREATE USER contractforge_iam;
GRANT rds_iam TO contractforge_iam;
GRANT CONNECT ON DATABASE postgres TO contractforge_iam;
GRANT USAGE ON SCHEMA public TO contractforge_iam;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO contractforge_iam;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO contractforge_iam;
```

Para validações rápidas, o usuário master também pode funcionar se estiver autorizado, mas o padrão recomendado é usuário dedicado com permissões mínimas.

## Policy IAM

O recurso de `rds-db:connect` usa o `DbiResourceId` ou `DbClusterResourceId`, não o ARN comum do cluster.

Para Aurora cluster:

```bash
aws rds describe-db-clusters \
  --db-cluster-identifier <cluster-id> \
  --query "DBClusters[0].DbClusterResourceId" \
  --output text
```

Policy exemplo:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds-db:connect",
      "Resource": "arn:aws:rds-db:us-east-1:123456789012:dbuser:cluster-ABCDEFGHIJKL/contractforge_iam"
    }
  ]
}
```

Anexe a policy ao usuário/role usado pelo job.

## Secrets

Exemplo de secrets no Databricks:

```bash
databricks secrets put-secret contractforge-aws rds_jdbc_url
databricks secrets put-secret contractforge-aws rds_username
databricks secrets put-secret contractforge-aws aws_access_key_id
databricks secrets put-secret contractforge-aws aws_secret_access_key
```

`aws_session_token` é opcional. Só declare quando estiver usando credenciais temporárias válidas. Token STS expirado causa falha de autenticação.

## AWS Credential Provider Chain

Quando o runtime já fornece credenciais AWS por instance profile, profile local, web identity, variável de ambiente gerenciada ou outro mecanismo suportado pelo `botocore`, use `credential_provider: default_chain`.

Esse modo exige `botocore` no driver Python. Instale a extra `contractforge[aws]` ou disponibilize `botocore` no ambiente. A ContractForge continua gerando o token IAM internamente; `boto3` e AWS CLI não são necessários.

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:contractforge-aws/rds_jdbc_url }}"
    dbtable: public.orders
    driver: org.postgresql.Driver
  auth:
    type: rds_iam
    username: "{{ secret:contractforge-aws/rds_username }}"
    region: us-east-1
    credential_provider: default_chain
```

Prioridade usada pelo conector:

1. Credenciais explícitas em `source.auth`.
2. Variáveis `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e `AWS_SESSION_TOKEN`.
3. `credential_provider: default_chain`, se configurado.

## Contrato YAML

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: "{{ secret:contractforge-aws/rds_jdbc_url }}"
    dbtable: public.orders
    driver: org.postgresql.Driver
    ssl: "true"
    sslmode: require
  auth:
    type: rds_iam
    username: "{{ secret:contractforge-aws/rds_username }}"
    region: us-east-1
    access_key_id: "{{ secret:contractforge-aws/aws_access_key_id }}"
    secret_access_key: "{{ secret:contractforge-aws/aws_secret_access_key }}"
    sslmode: require
  read:
    fetchsize: 1000
    partition_column: order_id
    lower_bound: 1
    upper_bound: 1000000
    num_partitions: 8

target:
  catalog: contractforge
  schema: bronze
  table: b_rds_orders

mode: scd1_hash_diff
hash_keys: [order_id]
quality_rules:
  not_null: [order_id]
  unique_key: [order_id]
```

## Contrato Python

Quando usar `ingest()` diretamente, informe `catalog` explicitamente. `target_schema` qualificado não substitui `catalog`.

```python
from contractforge import ingest

result = ingest(
    catalog="contractforge",
    target_schema="bronze",
    target_table="b_rds_orders",
    ctrl_schema="ops",
    source={
        "type": "connector",
        "connector": "postgres",
        "options": {
            "url": "{{ secret:contractforge-aws/rds_jdbc_url }}",
            "dbtable": "public.orders",
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": "require",
        },
        "auth": {
            "type": "rds_iam",
            "username": "{{ secret:contractforge-aws/rds_username }}",
            "region": "us-east-1",
            "access_key_id": "{{ secret:contractforge-aws/aws_access_key_id }}",
            "secret_access_key": "{{ secret:contractforge-aws/aws_secret_access_key }}",
            "sslmode": "require",
        },
        "read": {
            "fetchsize": 1000,
            "partition_column": "order_id",
            "lower_bound": 1,
            "upper_bound": 1000000,
            "num_partitions": 8,
        },
    },
    mode="scd1_hash_diff",
    hash_keys=["order_id"],
)
```

## Métricas

O retorno e `ctrl_ingestion_runs.source_metrics_json` registram:

- `jdbc_auth_configured=true`
- `jdbc_auth_type=rds_iam`
- `jdbc_rds_iam_token_generated=true`
- `jdbc_rds_region=<region>`
- `jdbc_rds_iam_credential_source=explicit|env|default_chain`
- `jdbc_ssl_enabled=true`
- `partitioned_read=true|false`
- `fetchsize=<valor>`

Tokens e secrets são redigidos em metadata, lineage e control tables.

## Troubleshooting

`PAM authentication failed`

- O usuário do banco não tem `rds_iam`.
- A IAM principal não tem `rds-db:connect`.
- O token foi gerado para usuário, host, porta ou região diferentes.
- O `aws_session_token` expirou.
- O banco está exigindo IAM auth e você tentou `auth.type: basic`.

`No suitable driver` ou `ClassNotFoundException`

- O driver JDBC não está instalado no cluster.
- Em cluster UC standard/shared, o Maven pode estar bloqueado por artifact allowlist.

Timeout ou `Connection refused`

- O runtime Databricks não alcança o endpoint RDS.
- Verifique VPC, peering, Transit Gateway, PrivateLink/NLB, security groups, firewall ou Aurora Express Internet Access Gateway.

`Catalog 'main' was not found`

- Informe `catalog` explicitamente no `ingest()`.
- Não dependa de catalog default em workspaces novos.

`Metastore storage root URL does not exist`

- Não tente criar catálogo novo sem managed location.
- Use um catálogo já existente ou crie o catálogo via UI/SQL com `MANAGED LOCATION`.

## Limitações Atuais

- `credential_provider: default_chain` depende de `botocore` instalado e das credenciais estarem realmente disponíveis no driver Python.
- A conectividade de rede com RDS/Aurora continua fora do escopo da lib.
