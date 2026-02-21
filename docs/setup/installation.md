# Instalação e Onboarding

Este guia cobre o onboarding de um novo desenvolvedor no framework de Data Warehouse Single-Node.

## 1) Pré-requisitos

- Python 3.11+
- Git
- Acesso ao repositório

## 2) Instalar o `uv`

No Linux/macOS:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

No Windows (PowerShell):

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Validar instalação:

```bash
uv --version
```

## 3) Clonar o projeto e entrar na pasta

```bash
git clone <url-do-repositorio>
cd single_node_dw
```

## 4) Sincronizar ambiente com `uv sync`

Com o `pyproject.toml` já configurado no projeto:

```bash
uv sync
```

## 5) Configurar variáveis de ambiente (`.env`)

Crie seu `.env` local a partir do template:

```bash
cp .env.example .env
```

Edite os valores conforme seu ambiente. Prefixos importantes:

- `REST_`: integrações HTTP/REST (`REST_BASE_URL`, `REST_TOKEN`)
- `PG_`: fonte PostgreSQL (`PG_HOST`, `PG_PORT`, `PG_DATABASE`, ...)
- `DW_`: destino do Data Warehouse (`DW_HOST`, `DW_DATABASE`, ...)
- `MSSQL_`, `ORACLE_`, `SQLITE_`: conectores SQL adicionais
- `FTP_`, `WEBDAV_`, `SSH_`: conectores de arquivos/transferência
- `MONGODB_`, `CASSANDRA_`, `NEO4J_`: conectores NoSQL
- `KAFKA_`, `AMQP_`, `NATS_`: conectores de mensageria/stream
- `GSHEETS_`, `SOAP_`: conectores SaaS/serviços

## 6) Validar instalação com testes (`pytest`)

No diretório `single_node_dw/`, você pode rodar todos os testes ou suítes específicas:

```bash
# Todos os testes (Conectores, Staging, Pipeline, CLI)
make make-tests-all

# Apenas conectores
make make-tests-conn

# Apenas staging e carga (loader/audit)
make make-tests-staging

# Apenas orquestrador de pipeline
make make-tests-pipeline
```

## 7) Primeiro Pipeline (Quick Start)

Para testar o fluxo completo usando o CLI:

```bash
# 1. Subir infraestrutura (Docker)
make infra-up

# 2. Testar conexão com o DW
make test-dw

# 3. Executar um pipeline de exemplo (HTTP -> DW)
uv run --with-requirements etl/requirements.txt python -m etl.cli run \
  --config etl/connections/examples/http_connector.example.json \
  --query "/users" \
  --source api_test \
  --table stg_users_test \
  --lake ./lake
```

## 8) Troubleshooting rápido

Se ocorrer `ModuleNotFoundError` na coleta dos testes, repita os comandos com `--with-requirements etl/requirements.txt` (conforme acima).
