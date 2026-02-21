import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from connections._logging import get_logger

logger = get_logger("staging.dw_schema")


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _polars_dtype_to_postgres(dtype: pl.DataType, engine: Engine | None = None) -> str:
    dtype_name = str(dtype)
    is_sqlite = engine and engine.dialect.name == "sqlite"

    if dtype_name in {"Utf8", "String"}:
        return "TEXT"

    if dtype_name in {"Int32", "Int64"}:
        return "BIGINT"

    if dtype_name in {"Float32", "Float64"}:
        return "DOUBLE PRECISION"

    if dtype_name == "Boolean":
        return "BOOLEAN"

    if dtype_name == "Date":
        return "DATE"

    if dtype_name.startswith("Datetime"):
        return "TIMESTAMP" if is_sqlite else "TIMESTAMP WITH TIME ZONE"

    return "TEXT"


def _table_exists(engine: Engine, schema: str | None, table_name: str) -> bool:
    if engine.dialect.name == "sqlite":
        query = text("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=:table_name)")
        params = {"table_name": table_name}
    else:
        query = text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name = :table_name
            )
            """
        )
        params = {"schema": schema, "table_name": table_name}

    with engine.connect() as connection:
        result = connection.execute(query, params)
        exists = result.scalar()

    return bool(exists)


def _existing_columns(engine: Engine, schema: str | None, table_name: str) -> set[str]:
    if engine.dialect.name == "sqlite":
        query = text(f"PRAGMA table_info({_quote_identifier(table_name)})")
        with engine.connect() as connection:
            result = connection.execute(query)
            columns = {row[1] for row in result.fetchall()}  # row[1] is name
        return columns

    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table_name
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query, {"schema": schema, "table_name": table_name})
        columns = {row[0] for row in result.fetchall()}

    return columns


def ensure_table_exists(
    engine: Engine,
    table_name: str,
    frame: pl.DataFrame,
    schema: str | None = "public",
) -> None:
    if schema:
        qualified_name = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
    else:
        qualified_name = _quote_identifier(table_name)

    is_sqlite = engine.dialect.name == "sqlite"
    ts_type = "TIMESTAMP" if is_sqlite else "TIMESTAMP WITH TIME ZONE"
    now_func = "CURRENT_TIMESTAMP" if is_sqlite else "now()"

    frame_columns = [
        (column_name, _polars_dtype_to_postgres(frame.schema[column_name], engine=engine))
        for column_name in frame.columns
    ]

    create_definitions = [
        f"{_quote_identifier(column_name)} {column_type}"
        for column_name, column_type in frame_columns
    ]
    create_definitions.append(f'_loaded_at {ts_type} DEFAULT {now_func}')
    create_definitions.append('_source_file TEXT')

    create_sql = f"CREATE TABLE IF NOT EXISTS {qualified_name} ({', '.join(create_definitions)})"

    with engine.connect() as connection:
        logger.info("Executing table ensure DDL", extra={"ddl": create_sql})
        connection.execute(text(create_sql))
        connection.commit()

    if not _table_exists(engine, schema=schema, table_name=table_name):
        return

    existing_columns = _existing_columns(engine, schema=schema, table_name=table_name)

    required_columns: list[tuple[str, str]] = list(frame_columns)
    required_columns.append(("_loaded_at", f"{ts_type} DEFAULT {now_func}"))
    required_columns.append(("_source_file", "TEXT"))

    new_columns = [
        (column_name, column_type)
        for column_name, column_type in required_columns
        if column_name not in existing_columns
    ]

    if not new_columns:
        return

    with engine.connect() as connection:
        for column_name, column_type in new_columns:
            alter_sql = (
                f"ALTER TABLE {qualified_name} "
                f"ADD COLUMN { _quote_identifier(column_name)} {column_type}"
            )
            logger.info("Adding missing column", extra={"ddl": alter_sql, "column": column_name})
            connection.execute(text(alter_sql))

        connection.commit()
