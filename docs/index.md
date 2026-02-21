# Framework de Data Warehouse Single-Node

Este projeto é um framework de Data Warehouse **Single-Node** projetado para entregar **baixo custo operacional** e **alta eficiência** em cenários de dados pequenos e médios.

A proposta é simples: reduzir complexidade de infraestrutura sem abrir mão de organização, rastreabilidade e qualidade no processo de ETL.

## Objetivo

- Consolidar dados de múltiplas fontes em um ambiente único.
- Facilitar evolução incremental do pipeline de ETL.
- Priorizar previsibilidade de custos e operação enxuta.

## Stack Técnica

- **Python** como linguagem principal de desenvolvimento dos pipelines e conectores.
- **uv** para gerenciamento de pacotes e execução consistente de scripts e testes.

Essa combinação permite setup rápido, dependências controladas e manutenção simplificada.

## Arquitetura

A arquitetura é dividida em três blocos claros:

1. **Conectores**
   - Responsáveis pela integração com fontes de dados (SQL, APIs, arquivos, streams e serviços externos).
   - Isolam detalhes de autenticação, leitura e normalização inicial.

2. **Staging em arquivos**
   - Camada intermediária para persistência temporária dos dados extraídos.
   - Melhora rastreabilidade, facilita reprocessamentos e desacopla ingestão de carga analítica.

3. **DW Relacional**
   - Camada final de armazenamento analítico estruturado.
   - Organiza dados para consumo por BI, relatórios operacionais e análises de negócio.

4. **Pipeline Orchestrator & CLI**
   - Motor de execução que coordena o fluxo fim-a-fim (Extração -> Staging -> Carga).
   - CLI unificada para execução de pipelines, testes de conexão e monitoramento via logs de auditoria.

## Resultado esperado

Com essa abordagem, o framework oferece uma base pragmática para ETL em ambiente single-node: implementação rápida, operação estável e custo controlado.
