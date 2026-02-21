# Arquitetura Single-Node

A arquitetura deste projeto adota um modelo **Single-Node**: ingestão, staging, transformação e carga analítica operam no mesmo ambiente Linux, com separação lógica por camadas.

## Visão do fluxo

```mermaid
flowchart LR
    A[Source Systems] --> B[Connectors Package]
    B --> C[Local Staging Area (Files)]
    C --> D[ETL Processing]
    D --> E[PostgreSQL DW]
```

## Componentes

- **Source Systems**: bancos relacionais, APIs, arquivos e serviços externos.
- **Connectors Package**: camada de integração que padroniza autenticação, leitura e contrato de dados.
- **Local Staging Area (Files)**: persistência intermediária em arquivos para desacoplar extração e transformação.
- **ETL Processing**: normalização, validações e aplicação de regras de negócio.
- **PostgreSQL DW**: armazenamento analítico relacional para BI, relatórios e consultas.

## Por que uma única VM Linux

Escolhemos executar toda a solução em uma única VM Linux para reduzir custo e complexidade operacional:

- **Menor custo de infraestrutura**: evita gastos com clusterização e múltiplos serviços distribuídos.
- **Operação simplificada**: provisioning, monitoramento, backup e troubleshooting em um único ponto.
- **Menor latência interna**: troca de dados entre camadas no mesmo host, sem dependência de rede entre nós.
- **Onboarding rápido**: ambiente previsível para desenvolvimento e suporte.
- **Escala adequada ao contexto**: atende cargas pequenas e médias com boa eficiência.

Esse desenho prioriza pragmatismo: arquitetura clara, custo controlado e manutenção enxuta.
