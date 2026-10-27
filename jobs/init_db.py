"""Create the warehouse schema by running the SQL files in sql/schema."""

from pathlib import Path

from jobs.common.db import connect

SCHEMA_DIR = Path("sql/schema")


def schema_files(schema_dir: Path = SCHEMA_DIR) -> list[Path]:
    """SQL files in name order: the number prefix sets the order."""
    return sorted(schema_dir.glob("*.sql"))


def main() -> None:
    files = schema_files()
    # One transaction for every file: leaving the block commits, and an error
    # in any statement rolls everything back. PostgreSQL can roll back DDL.
    with connect() as conn:
        for path in files:
            # Without query parameters, psycopg accepts several statements.
            conn.execute(path.read_text(encoding="utf-8"))
            print(f"Ran {path.as_posix()}")
    print(f"Committed {len(files)} schema files")


if __name__ == "__main__":
    main()
