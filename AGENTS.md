# Data Warehouses building in diverse Scenarios

## Rules
- Always use pytest to unit tests
- Always install and use uv for lib management and run scripts
- Always when building python functions, create with clear and positional arguments (avoid args and kwargs)
- Simple Functions instead of classes
- Always expose errors to a better debug
- Always log every scripts
- Sparse instead of dense to a better legibility

## Test Execution Rules (avoid import/dependency errors)
- Always run tests with uv injecting pytest and project requirements in the same command.
- From `single_node_dw/`, use: `uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py`
- For focused runs, use: `uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_connections.py -k <test_name>`
- Never use `uv run pytest ...` alone in this repository (it may fail when pytest is not installed in the environment).
- If any `ModuleNotFoundError` appears during collection, re-run with `--with-requirements etl/requirements.txt` before changing code.