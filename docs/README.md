# Documentação do ContractForge

Este diretório concentra a documentação técnica do ContractForge. Para leitura navegável, use também a documentação web:

https://marquesantero.github.io/contractforge/

## Comece Aqui

- [Quickstart](quickstart.md): fluxo mínimo para validar instalação, executar uma ingestão e consultar ctrl tables.
- [Documentação oficial](oficial.md): referência completa de uso, contratos, modos, conectores, shape, governança e observabilidade.
- [Guia de uso](guia_de_uso.md): passo a passo operacional para pacote, notebooks, YAMLs e Databricks Workflows.

## Referência Técnica

- [Arquitetura](arquitetura.md): módulos internos, fluxo de execução, edge cases e decisões de design.
- [ADRs](adrs/README.md): decisões arquiteturais formais.
- [Changelog](../CHANGELOG.md): histórico de versões e política de release.

## Guias Por Tema

- [Compatibilidade de conectores](compatibilidade_conectores.md): matriz de conectores, dependências e suporte por runtime.
- [Operação e manutenção](operacao.md): retenção das ctrl tables, limpeza/VACUUM e práticas operacionais.
- [Performance](performance.md): guidelines por modo de escrita, cache, JDBC, REST, Delta layout e métricas.
- [Segurança](seguranca.md): práticas para secrets, explain, lineage, ctrl tables e quarentena.
- [Anti-patterns](antipadroes.md): configurações perigosas e alternativas recomendadas.
- [Template de projeto](template_projeto.md): estrutura recomendada com contratos, notebooks e Databricks Asset Bundles.

## Site

O conteúdo navegável do GitHub Pages é publicado pela branch `gh-pages`:

https://marquesantero.github.io/contractforge/

Os arquivos Markdown deste diretório são a fonte técnica versionada no repositório.
