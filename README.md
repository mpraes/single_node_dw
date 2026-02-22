# Single Node DW

Framework de Data Warehouse single-node para pipelines ETL com conectores de origem e destino.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Características Principais](#características-principais)
- [Conectores Disponíveis](#conectores-disponíveis)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
- [Exemplos Práticos](#exemplos-práticos)
- [Integração com Mage.ai](#integração-com-mageai)
- [Testes](#testes)
- [Infraestrutura](#infraestrutura)
- [Documentação](#documentação)
- [Estrutura do Projeto](#estrutura-do-projeto)

## 🎯 Visão Geral

O **Single Node DW** é um framework Python moderno projetado para cenários de ETL que requerem **baixo custo operacional** e **alta eficiência** em ambientes de dados pequenos e médios. 

A proposta é reduzir a complexidade de infraestrutura sem comprometer a organização, rastreabilidade e qualidade do processo de ETL.

### Objetivos

- **Consolidação de dados**: Integrar múltiplas fontes em um ambiente único e estruturado
- **Evolução incremental**: Facilitar a expansão gradual do pipeline de ETL
- **Custo controlado**: Priorizar previsibilidade de custos e operação enxuta
- **Simplicidade operacional**: Minimizar overhead de infraestrutura e manutenção

## 🏗️ Arquitetura

### Componentes Principais

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FONTES DE     │    │     STAGING      │    │   DATA          │
│     DADOS       │────│   (Parquet)      │────│  WAREHOUSE      │
│                 │    │                  │    │ (PostgreSQL)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CONECTORES    │    │   PARQUET LAKE   │    │   TABELAS DW    │
│  - SQL (PG/MS)  │    │  - Versionamento │    │  - Schema auto  │
│  - HTTP/REST    │    │  - Auditoria     │    │  - Auditoria    │
│  - MongoDB      │    │  - Reprocesso    │    │  - Lineage      │
│  - Kafka/AMQP   │    │                  │    │                 │
│  - FTP/SSH      │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Fluxo de Processamento

1. **Extração**: Conectores especializados coletam dados das fontes
2. **Staging**: Dados são persistidos em formato Parquet para rastreabilidade
3. **Transformação**: Dados são normalizados e estruturados
4. **Carga**: Dados são inseridos no Data Warehouse PostgreSQL
5. **Auditoria**: Metadados de execução são registrados para monitoramento

### Stack Tecnológica

- **Python 3.11+**: Linguagem principal
- **uv**: Gerenciamento de dependências e execução
- **Polars**: Manipulação eficiente de DataFrames
- **SQLAlchemy 2.0+**: ORM e abstração de banco de dados
- **PostgreSQL**: Data Warehouse de destino
- **Parquet**: Formato de staging para dados
- **Pydantic**: Validação de configurações
- **Mage.ai**: Orquestração de pipelines (opcional)

## ✨ Características Principais

### 🔌 Conectores Extensíveis

Sistema de conectores baseado em protocolo com descoberta automática de classes:

```python
# Factory pattern com autodescoberta
connector = create_connector({
    "protocol": "postgres",
    "host": "localhost",
    "database": "analytics"
})
```

### 📦 Staging Inteligente

Camada intermediária usando Parquet para:
- Rastreabilidade completa de dados
- Capacidade de reprocessamento
- Desacoplamento entre ingestão e carga
- Versionamento automático por timestamp

### 🔍 Auditoria Completa

Sistema de auditoria integrado que registra:
- Execuções de pipeline com UUID único
- Contagem de linhas processadas
- Duração de execução
- Status de sucesso/falha
- Metadados de fonte e destino

### 🚀 CLI Unificada

Interface de linha de comando para:
- Execução de pipelines ETL
- Testes de conectividade
- Monitoramento via logs estruturados

### 📊 Schema Auto-adaptativo

Criação automática de esquemas no DW baseado na estrutura dos dados de entrada.

## 🔌 Conectores Disponíveis

### Bancos de Dados SQL
- **PostgreSQL**: Conectividade nativa com psycopg
- **Microsoft SQL Server**: Suporte via pyodbc
- **Oracle**: Conectividade com oracledb
- **SQLite**: Para desenvolvimento e testes

### APIs e Web Services
- **HTTP/REST**: Cliente HTTP com autenticação
- **SOAP**: Integração com serviços SOAP via zeep

### NoSQL
- **MongoDB**: Conectividade nativa com serialização de ObjectId
- **Cassandra**: Suporte a clusters Cassandra
- **Neo4j**: Conectividade via driver Bolt

### Arquivos e Transferência
- **FTP/SFTP**: Transferência de arquivos via FTP e SSH
- **WebDAV**: Integração com serviços WebDAV
- **SSH/SFTP**: Acesso seguro via paramiko

### Streaming e Messaging
- **Apache Kafka**: Consumo de mensagens via confluent-kafka
- **RabbitMQ (AMQP)**: Mensageria via pika
- **NATS**: Streaming de mensagens via nats-py

### SaaS e Cloud Services
- **Google Sheets**: Integração via gspread com Service Account

## 📦 Instalação

### Requisitos

- Python 3.11+
- `uv` (gerenciador de pacotes)

### Instalação

No diretório do projeto:

```bash
uv sync --group dev
```

## ⚙️ Configuração

### Variáveis de Ambiente

Crie um arquivo `.env` no diretório raiz:

```bash
# Data Warehouse (Destino)
DW_HOST=localhost
DW_PORT=5432
DW_DATABASE=dw_db
DW_USERNAME=dw_user
DW_PASSWORD=dw_password

# Exemplo: Fonte PostgreSQL
PG_SOURCE_HOST=source-db.example.com
PG_SOURCE_PORT=5432
PG_SOURCE_DATABASE=prod_db
PG_SOURCE_USERNAME=readonly_user
PG_SOURCE_PASSWORD=secure_password
```

### Arquivos de Configuração

Os conectores podem ser configurados via JSON ou YAML:

#### PostgreSQL (postgres_connector.yaml)
```yaml
protocol: postgres
host: source-db.example.com
port: 5432
database: production
username: etl_user
password: secure_password
```

#### HTTP/REST (api_connector.yaml)
```yaml
protocol: http
base_url: https://api.example.com
token: your_api_token
headers:
  Content-Type: application/json
  X-Custom-Header: custom_value
```

#### MongoDB (mongo_connector.yaml)
```yaml
protocol: mongodb
host: mongo.example.com
port: 27017
database: analytics
username: mongo_user
password: mongo_password
auth_source: admin
```

Veja mais exemplos em [`etl/connections/examples/`](etl/connections/examples/).

## 🚀 Uso

### CLI Principal

#### Executar Pipeline ETL

```bash
cd etl && uv run --with-requirements requirements.txt python -m etl.cli run \
  --config ../config/postgres_source.yaml \
  --query "SELECT * FROM users WHERE updated_at > '2024-01-01'" \
  --source "postgres_prod" \
  --table "stg_users" \
  --lake ../lake \
  --schema "staging" \
  --pipeline "daily_user_sync"
```

#### Testar Conexões

```bash
# Testar conexão do Data Warehouse
cd etl && uv run --with-requirements requirements.txt python -m etl.cli test-connection --source dw

# Testar conexão de fonte
cd etl && uv run --with-requirements requirements.txt python -m etl.cli test-connection --config ../config/postgres_source.yaml
```

### Via Makefile (Recomendado)

```bash
# Executar pipeline
make run-pipeline CONFIG=config/postgres_source.yaml QUERY="SELECT * FROM users" SOURCE=postgres_prod TABLE=stg_users

# Testar conexão DW
make test-dw

# Executar todos os testes
make make-tests-all
```

### Programaticamente

```python
from etl.pipeline.runner import run_pipeline
from etl.connections.dw_destination import get_dw_engine

# Configuração do conector
config = {
    "protocol": "postgres",
    "host": "source-db.com",
    "database": "production",
    "username": "etl_user",
    "password": "password"
}

# Engine do DW
dw_engine = get_dw_engine()

# Executar pipeline
result = run_pipeline(
    connector_config=config,
    query="SELECT * FROM orders WHERE date >= '2024-01-01'",
    source_name="prod_orders",
    target_table="stg_orders",
    lake_path="./lake",
    dw_engine=dw_engine,
    schema="staging"
)

print(f"Pipeline status: {result['status']}")
print(f"Rows loaded: {result['rows_loaded']}")
```

## 📚 Exemplos Práticos

### 1. Sincronização PostgreSQL para DW

```python
# examples/postgres_to_dw.py
from etl.pipeline.runner import run_pipeline
from etl.connections.dw_destination import get_dw_engine

connector_config = {
    "protocol": "postgres",
    "env_prefix": "PG_SOURCE"  # Usa PG_SOURCE_HOST, PG_SOURCE_USER, etc.
}

dw_engine = get_dw_engine()

result = run_pipeline(
    connector_config=connector_config,
    query="SELECT * FROM public.users",
    source_name="postgres_prod",
    target_table="stg_users",
    lake_path="./lake",
    dw_engine=dw_engine,
    pipeline_name="postgres_sync"
)
```

### 2. API REST para DW

```python
# examples/rest_api_to_dw.py
connector_config = {
    "protocol": "http",
    "base_url": "https://jsonplaceholder.typicode.com",
}

result = run_pipeline(
    connector_config=connector_config,
    query="/users",  # endpoint da API
    source_name="placeholder_api",
    target_table="stg_api_users",
    lake_path="./lake",
    dw_engine=dw_engine,
    pipeline_name="api_sync"
)
```

### 3. Pipeline Incremental

```python
# examples/incremental_postgres_to_dw.py
from etl.connections.sources.sql.incremental import fetch_incremental_rows

# Busca apenas registros novos baseado em watermark
rows, new_watermark = fetch_incremental_rows(
    engine=source_engine,
    table_name="orders",
    watermark_column="updated_at",
    last_watermark=datetime(2024, 1, 1),
    batch_size=1000
)
```

## 🎭 Integração com Mage.ai

O framework inclui integração completa com Mage.ai para orquestração visual de pipelines.

### Setup da Integração

```bash
# Subir infraestrutura (PostgreSQL + Mage)
make infra-up

# Testar integração
make integration-test
```

### Blocos Mage Customizados

#### Data Loader
```python
# mage_blocks/data_loaders/etl_source_extractor.py
@data_loader
def extract_data(*args, **kwargs):
    return execute_etl_extraction(
        config_path="/app/configs/postgres_source.json",
        query="SELECT * FROM users WHERE active = true",
        source_name="active_users"
    )
```

#### Data Exporter
```python
# mage_blocks/data_exporters/etl_dw_loader.py
@data_exporter
def load_to_dw(data, *args, **kwargs):
    return load_to_data_warehouse(
        data=data,
        target_table="stg_active_users",
        schema="staging"
    )
```

### Acesso à Interface

Após iniciar a infraestrutura:
- **Mage.ai UI**: http://localhost:6789
- **PostgreSQL**: localhost:5432

## 🧪 Testes

### Estrutura de Testes

- **Testes unitários**: Validação de componentes individuais
- **Testes de integração**: Validação de fluxos completos
- **Testes de conexão**: Validação de conectividade com fontes

### Executar Testes

```bash
# Todos os testes
make make-tests-all

# Testes específicos
make make-tests-conn      # Testes de conexão
make make-tests-staging   # Testes de staging
make make-tests-pipeline  # Testes de pipeline

# Teste específico
uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py -k test_postgres
```

### Testes de Integração com Mage

```bash
# Testar integração completa
make integration-test

# Apenas integração Mage
make test-mage-integration
```

## 🏗️ Infraestrutura

### Docker Compose

O projeto inclui um `docker-compose.yml` completo com:

- **PostgreSQL 16**: Data Warehouse de destino
- **Mage.ai**: Orquestrador visual de pipelines
- **Volumes persistentes**: Para dados e configurações
- **Networking**: Conectividade entre serviços

### Comandos de Infraestrutura

```bash
# Iniciar serviços
make infra-up

# Parar serviços
make infra-down

# Status dos serviços
make infra-status
```

### Terraform (Opcional)

Configurações para deploy em cloud:

- **AWS**: `infra/aws.tf`
- **Azure**: `infra/azure.tf`
- **Variáveis**: `infra/terraform.tfvars.example`

## 📖 Documentação

### Gerar Documentação

```bash
# Servir documentação localmente
make docs-serve  # Acesse http://localhost:8000

# Gerar site estático
make docs-build  # Saída em docs/site/

# Limpar documentação
make docs-clean
```

### Guias Disponíveis

- **Instalação**: `docs/setup/installation.md`
- **Arquitetura**: `docs/setup/architecture.md`
- **Novo Conector**: `docs/guides/new-connector.md`
- **Pipeline**: `docs/guides/pipeline.md`
- **Orquestração Mage**: `docs/guides/mage-orchestration.md`

## 📁 Estrutura do Projeto

```
single_node_dw/
├── etl/                          # Framework ETL principal
│   ├── connections/              # Sistema de conectores
│   │   ├── sources/              # Conectores de origem
│   │   │   ├── sql/              # Bancos SQL
│   │   │   ├── http/             # APIs REST/HTTP
│   │   │   ├── nosql/            # Bancos NoSQL
│   │   │   ├── streams/          # Streaming (Kafka, AMQP)
│   │   │   ├── ftp/              # FTP/WebDAV
│   │   │   ├── ssh/              # SSH/SFTP
│   │   │   └── saas/             # SaaS (Google Sheets)
│   │   ├── examples/             # Exemplos de configuração
│   │   └── dw_destination.py     # Conexão DW destino
│   ├── pipeline/                 # Motor de execução
│   ├── staging/                  # Camada de staging
│   └── cli.py                    # Interface CLI
├── examples/                     # Exemplos de uso
├── mage_blocks/                  # Blocos customizados Mage
├── mage_templates/               # Templates de pipeline
├── tests/                        # Suite de testes
│   └── integration/              # Testes de integração
├── docs/                         # Documentação
├── infra/                        # Infraestrutura como código
├── lake/                         # Data Lake (Parquet)
├── docker-compose.yml            # Orquestração de serviços
├── Makefile                      # Comandos de automação
└── pyproject.toml               # Configuração do projeto
```

### Componentes Principais

#### Framework ETL (`etl/`)
- **`connections/`**: Sistema extensível de conectores
- **`pipeline/`**: Motor de execução de pipelines
- **`staging/`**: Camada de staging com Parquet
- **`cli.py`**: Interface de linha de comando

#### Conectores (`etl/connections/sources/`)
- **`factory.py`**: Factory pattern para instanciação
- **`base_connector.py`**: Interface abstrata de conectores
- **`data_contract.py`**: Contratos de dados padronizados

#### Staging (`etl/staging/`)
- **`writer.py`**: Escrita de dados em Parquet
- **`loader.py`**: Carregamento de Parquet para DW
- **`audit.py`**: Sistema de auditoria
- **`dw_schema.py`**: Gerenciamento de esquemas DW

#### Mage Integration (`mage_blocks/`)
- **`custom/`**: Blocos personalizados
- **`data_loaders/`**: Extratores de dados
- **`data_exporters/`**: Carregadores de dados

## 🔧 Desenvolvimento

### Criando um Novo Conector

1. **Crie o módulo do conector**:
```python
# etl/connections/sources/myprotocol/connector.py
from ..base_connector import BaseConnector
from ..data_contract import IngestionResult

class MyProtocolConnector(BaseConnector):
    def connect(self):
        # Implementar lógica de conexão
        pass
    
    def fetch_data(self, query: str) -> IngestionResult:
        # Implementar lógica de extração
        pass
    
    def close(self):
        # Implementar limpeza de recursos
        pass
```

2. **Configure exemplo**:
```yaml
# etl/connections/examples/myprotocol_connector.example.yaml
protocol: myprotocol
endpoint: https://api.example.com
api_key: your_api_key
```

3. **Teste o conector**:
```python
# tests/test_myprotocol.py
def test_myprotocol_connector():
    connector = create_connector({
        "protocol": "myprotocol",
        "endpoint": "https://api.test.com"
    })
    assert isinstance(connector, MyProtocolConnector)
```

### Contribuindo

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-funcionalidade`)
3. Implemente as mudanças com testes
4. Execute a suite de testes (`make make-tests-all`)
5. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
6. Push para a branch (`git push origin feature/nova-funcionalidade`)
7. Abra um Pull Request

---

## 📝 Licença

Este projeto está licenciado sob os termos da licença MIT.

## 🤝 Suporte

Para dúvidas, problemas ou sugestões:

1. Abra uma [issue](../../issues)
2. Consulte a [documentação](docs/)
3. Execute os testes de diagnóstico: `make integration-test`

---

**Feito com ❤️ para simplificar ETL em ambientes single-node**