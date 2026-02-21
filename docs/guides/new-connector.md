# Guia: Criar um Novo Conector

Este guia usa o padrão do `HTTPConnector` como referência de estilo e estrutura para novos conectores no pacote `etl/connections/sources`.

## Objetivo do padrão

Todo conector deve:

- Implementar a interface `BaseConnector` (`connect`, `fetch_data`, `close`).
- Retornar dados no contrato `IngestionResult` com itens `IngestedItem`.
- Usar configuração em camadas via `load_connection_config`.
- Validar configuração com Pydantic (`<Protocol>Config.model_validate(...)`).
- Registrar logs com `get_logger(...)` e mascaramento com `redact_config(...)`.
- Expor erros claros para facilitar debugging.

## Estrutura recomendada

Crie um diretório de protocolo em `etl/connections/sources/<protocol>/` com:

- `config.py`: modelo Pydantic de configuração.
- `connector.py`: implementação do conector.
- `__init__.py`: exportações públicas (quando aplicável).

## Passo 1: Definir o modelo de configuração

Exemplo (`config.py`):

```python
from pydantic import BaseModel, ConfigDict, Field


class NewProtocolConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    endpoint: str
    api_key: str | None = None
    timeout_seconds: int = Field(default=30, ge=1)
```

## Passo 2: Implementar o conector

Exemplo base (`connector.py`) no mesmo estilo do `HTTPConnector`:

```python
from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import NewProtocolConfig


class NewProtocolConnector(BaseConnector):
    def __init__(
        self,
        endpoint: str,
        api_key: str | None = None,
        timeout_seconds: int = 30,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "NEWPROTOCOL",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("endpoint",),
            defaults={
                "timeout_seconds": timeout_seconds,
            },
            overrides={
                "endpoint": endpoint,
                "api_key": api_key,
                "timeout_seconds": timeout_seconds,
            },
        )
        self.config = NewProtocolConfig.model_validate(merged_config)
        self.logger = get_logger("sources.newprotocol.connector")
        self._client = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting NewProtocol connector with config=%s", safe_config)

        self._client = object()
        self.logger.info("NewProtocol connector ready")

    def fetch_data(self, query: str) -> IngestionResult:
        if not query.strip():
            raise ValueError("query cannot be empty.")

        if self._client is None:
            raise RuntimeError("NewProtocol connector is not connected. Call connect() first.")

        payload = {"endpoint": self.config.endpoint, "query": query, "status": "ok"}

        return IngestionResult(
            protocol="newprotocol",
            success=True,
            items=[IngestedItem(payload=payload)],
            metadata={"client": "default"},
        )

    def close(self) -> None:
        self.logger.info("Closing NewProtocol connector")
        self._client = None
```

## Passo 3: Convenções obrigatórias

- Use argumentos explícitos e nomeados no `__init__` (evite `*args` e `**kwargs`).
- Defina `env_prefix` consistente com o protocolo (ex.: `REST`, `PG`, `MONGODB`).
- Garanta mensagens de erro orientadas à ação (`Call connect() first`, campo obrigatório ausente, etc.).
- Mantenha o ciclo de vida completo: abrir recursos em `connect()` e liberar em `close()`.

## Passo 4: Exemplo de variáveis de ambiente

Para `env_prefix="NEWPROTOCOL"`, o loader mapeia campos como:

```bash
NEWPROTOCOL_ENDPOINT=https://api.example.com
NEWPROTOCOL_API_KEY=token-value
NEWPROTOCOL_TIMEOUT_SECONDS=30
```

## Passo 5: Usar via factory dinâmica

Com `protocol` configurado, o conector pode ser criado por `create_connector`:

```python
from connections.sources.factory import create_connector

connector = create_connector(
    {
        "protocol": "newprotocol",
        "endpoint": "https://api.example.com",
        "api_key": "token-value",
    }
)

connector.connect()
result = connector.fetch_data("/health")
connector.close()
```

## Passo 6: Validar com testes

Rode os testes do projeto com `uv` + `pytest`:

```bash
cd single_node_dw
uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py
```

Para focar em um caso específico:

```bash
uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py -k newprotocol
```

## Checklist de revisão

- Contrato `BaseConnector` implementado.
- Configuração validada com Pydantic.
- Logs com redaction de dados sensíveis.
- `IngestionResult` preenchido com `protocol`, `success`, `items` e `metadata`.
- `connect()` e `close()` sem vazamento de recurso.
- Erros explícitos e fáceis de diagnosticar.
