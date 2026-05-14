# Anti-patterns

Esta página lista configurações que parecem válidas, mas costumam gerar perda de dados, custo excessivo ou baixa governança.

## Snapshot Incremental

Errado:

```yaml
mode: snapshot_soft_delete
watermark_columns: updated_at
merge_keys: device_id
```

`snapshot_soft_delete` precisa de fonte completa. Se a fonte é filtrada por watermark, a lib não consegue distinguir registro ausente de registro não lido.

Use:

```yaml
mode: snapshot_soft_delete
merge_keys: device_id
source:
  type: connector
  connector: table
  table: main.raw.devices_snapshot
  read:
    source_complete: true
```

## Secrets Literais no YAML

Errado:

```yaml
source:
  type: connector
  connector: postgres
  options:
    url: jdbc:postgresql://host/db
    user: app_user
    password: senha-em-texto
```

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

O ContractForge redige metadados antes de persistir em ctrl tables, mas não deve receber segredo literal quando existe secret manager.

## Explode Cedo Demais

Evite alterar cardinalidade em Bronze sem intenção explícita:

```yaml
layer: bronze
shape:
  arrays:
    items:
      mode: explode
```

Prefira preservar o payload bruto em Bronze e fazer normalização em Silver:

```yaml
layer: silver
shape:
  allow_cardinality_change_on_bronze: false
  arrays:
    items:
      mode: explode_outer
```

## Merge Key Nula

Errado:

```yaml
mode: scd1_upsert
merge_keys: customer_id
quality_rules:
  not_null: []
```

Use quality gate explícito:

```yaml
mode: scd1_upsert
merge_keys: customer_id
quality_rules:
  not_null: [customer_id]
  unique_key: [customer_id]
```

Chaves nulas em `MERGE` tornam o resultado difícil de auditar e podem esconder problemas de origem.

## Explain Permanente em Produção

Errado:

```yaml
explain_mode: true
explain_format: formatted
```

Use `explain_mode` em desenvolvimento, CI ou diagnóstico pontual. Em execução contínua, o plano Spark pode ser grande e custoso.

## REST API Como Carga Massiva

Errado:

```yaml
source:
  type: connector
  connector: rest_api
  request:
    url: https://api.example.com/all-events
  limits:
    max_pages: 100000
```

Para volumes altos, descarregue primeiro em storage e ingira por Auto Loader:

```yaml
source:
  type: connector
  connector: autoloader
  format: json
  path: /Volumes/main/landing/events
  read:
    schema_location: /Volumes/main/ops/schemas/events
    checkpoint_location: /Volumes/main/ops/checkpoints/events
```

## Reconcile Agressivo de Access

Evite começar com revogação automática:

```yaml
access_policy:
  mode: apply
  on_drift: reconcile
  revoke_unmanaged: true
```

Prefira validar primeiro:

```yaml
access_policy:
  mode: validate_only
  on_drift: warn
  revoke_unmanaged: false
```

Quando a revogação for necessária, use `contractforge apply-access --force-revoke` em janela controlada.
