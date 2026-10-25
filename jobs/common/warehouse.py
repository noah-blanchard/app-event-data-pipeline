"""Load Spark DataFrames into PostgreSQL tables, idempotently."""

from datetime import date, timedelta

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame

from jobs.common.db import connect, jdbc_options


def create_daily_partitions(table: str, start: date, end: date) -> None:
    """Create the missing one-day partitions of table, from start to end.

    Must run before the rows are inserted: once rows of a day sit in the
    DEFAULT partition, PostgreSQL refuses to create that day's partition.
    """
    schema, name = table.split(".")
    with connect() as conn:
        day = start
        while day <= end:
            # DDL takes no query parameters: values are inlined as literals.
            conn.execute(
                sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {} PARTITION OF {} "
                    "FOR VALUES FROM ({}) TO ({})"
                ).format(
                    sql.Identifier(schema, f"{name}_{day:%Y%m%d}"),
                    sql.Identifier(schema, name),
                    sql.Literal(day),
                    sql.Literal(day + timedelta(days=1)),
                )
            )
            day += timedelta(days=1)


def column_types(conn: psycopg.Connection, table: str) -> list[tuple[str, str]]:
    """(name, SQL type) of every column of table, in table order."""
    rows = conn.execute(
        """
        SELECT attname, format_type(atttypid, atttypmod)
        FROM pg_attribute
        WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
        """,
        (table,),
    ).fetchall()
    return [(name, type_) for name, type_ in rows]


def replace_rows(
    df: DataFrame, table: str, key_column: str, start: date, end: date
) -> int:
    """Replace the rows of table whose key_column is between start and end
    with the rows of df, atomically. Returns the number of rows inserted.

    Spark's JDBC writer can only append or overwrite a whole table, so df
    goes to a staging table first. Then one transaction deletes the old rows
    and copies the new ones: readers see the old day or the new day, never a
    half-loaded one, and running it twice gives the same result.
    """
    schema, name = table.split(".")
    staging = sql.Identifier(schema, f"{name}_staging")
    # Recreated by Spark on every run, with Spark's types (no UUID, no CHAR).
    (
        df.write.format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", f"{schema}.{name}_staging")
        .mode("overwrite")
        .save()
    )

    with connect() as conn:
        columns = column_types(conn, table)
        names = sql.SQL(", ").join(sql.Identifier(c) for c, _ in columns)
        # Cast each staging column to the type of the target column.
        casts = sql.SQL(", ").join(
            sql.SQL("{}::{}").format(sql.Identifier(c), sql.SQL(t)) for c, t in columns
        )
        conn.execute(
            sql.SQL("DELETE FROM {} WHERE {} BETWEEN %s AND %s").format(
                sql.Identifier(schema, name), sql.Identifier(key_column)
            ),
            (start, end),
        )
        inserted = conn.execute(
            sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                sql.Identifier(schema, name), names, casts, staging
            )
        ).rowcount
        # Transit data only: drop it with the same commit.
        conn.execute(sql.SQL("DROP TABLE {}").format(staging))
    return inserted
