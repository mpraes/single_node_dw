from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import MetaData, Table, asc, select
from sqlalchemy.engine import Engine

from ..._logging import get_logger

LOGGER = get_logger("sources.sql.incremental")

WatermarkValue = int | float | str | datetime | date | Decimal | None


def _resolve_table(engine: Engine, table_name: str, schema: str | None) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, schema=schema, autoload_with=engine)


def fetch_incremental_rows(
    engine: Engine,
    table_name: str,
    watermark_column: str,
    last_watermark: WatermarkValue,
    batch_size: int = 1000,
    schema: str | None = None,
) -> tuple[list[dict], WatermarkValue]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    table = _resolve_table(engine, table_name, schema)
    if watermark_column not in table.c:
        raise ValueError(f"watermark column '{watermark_column}' not found in table '{table_name}'")

    watermark_field = table.c[watermark_column]
    statement = select(table).order_by(asc(watermark_field)).limit(batch_size)

    if last_watermark is not None:
        statement = statement.where(watermark_field > last_watermark)

    LOGGER.info(
        "Running incremental SQL load table=%s schema=%s watermark_column=%s batch_size=%s",
        table_name,
        schema,
        watermark_column,
        batch_size,
    )

    with engine.connect() as connection:
        rows = [dict(row._mapping) for row in connection.execute(statement)]

    if not rows:
        LOGGER.info("No incremental rows found for table=%s", table_name)
        return [], last_watermark

    new_watermark = rows[-1].get(watermark_column, last_watermark)
    LOGGER.info(
        "Incremental SQL load finished table=%s rows=%s new_watermark=%s",
        table_name,
        len(rows),
        new_watermark,
    )
    return rows, new_watermark
