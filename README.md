# Single Node DW

Framework de Data Warehouse single-node para pipelines ETL com conectores de origem e destino.

## Requisitos

- Python 3.11+
- `uv`

## Instalação

No diretório do projeto:

```bash
uv sync --group dev
```

## Documentação

Comandos via `Makefile`:

```bash
make docs-serve
make docs-build
make docs-clean
```

Saída gerada:

- `make docs-build` publica o site estático em `docs/site`.

### Atualização recente (compatibilidade MkDocs)

Para evitar warning de incompatibilidade entre MkDocs 2.x e Material for MkDocs, os comandos de documentação foram ajustados para usar o grupo de dependências de desenvolvimento e versões compatíveis:

- `mkdocs>=1.6.0,<2.0`
- `mkdocs-material>=9.5.0,<9.7`
- `docs-serve` e `docs-build` executam `uv run --group dev ...`

## Testes

Executar os testes conforme o padrão do projeto:

```bash
uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py
```

Ou via `Makefile` (carrega `.env` quando existir):

```bash
make make-tests-conn
```
