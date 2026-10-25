"""Shared PostgreSQL connection: every job reads the same settings."""

import os

import psycopg


def connect() -> psycopg.Connection:
    """Open a connection to the warehouse from the POSTGRES_* variables.

    The defaults match docker-compose.yml. Inside the compose network the host
    is "postgres"; from the host machine it is localhost, through the
    published port.
    """
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline"),
        dbname=os.environ.get("POSTGRES_DB", "warehouse"),
    )
