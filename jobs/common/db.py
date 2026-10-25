"""Shared PostgreSQL connection: every job reads the same settings."""

import os

import psycopg

# The defaults match docker-compose.yml. Inside the compose network the host
# is "postgres"; from the host machine it is localhost, through the published
# port.
DEFAULTS = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_USER": "pipeline",
    "POSTGRES_PASSWORD": "pipeline",
    "POSTGRES_DB": "warehouse",
}


def setting(name: str) -> str:
    """Value of a POSTGRES_* variable, or its default."""
    return os.environ.get(name, DEFAULTS[name])


def connect() -> psycopg.Connection:
    """Open a connection to the warehouse from the POSTGRES_* variables."""
    return psycopg.connect(
        host=setting("POSTGRES_HOST"),
        port=setting("POSTGRES_PORT"),
        user=setting("POSTGRES_USER"),
        password=setting("POSTGRES_PASSWORD"),
        dbname=setting("POSTGRES_DB"),
    )


def jdbc_options() -> dict[str, str]:
    """The same connection, as options for Spark's JDBC writer."""
    host, port, db = (setting(f"POSTGRES_{n}") for n in ("HOST", "PORT", "DB"))
    return {
        # rewriteBatchedInserts: the driver sends a batch of rows as one
        # multi-row INSERT instead of one statement per row, much faster.
        "url": f"jdbc:postgresql://{host}:{port}/{db}?rewriteBatchedInserts=true",
        "user": setting("POSTGRES_USER"),
        "password": setting("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }
