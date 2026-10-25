# Pipeline runner: Python 3.12, Java 17 (required by Spark) and uv.
# Bookworm (Debian 12) is pinned because it still ships OpenJDK 17.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# PostgreSQL JDBC driver: Spark needs it to write to the warehouse. The
# checksum makes the build fail if the downloaded file is not the expected one.
ADD --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/jars/
ENV POSTGRES_JDBC_JAR=/opt/jars/postgresql-42.7.14.jar

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

# Keep the virtual environment outside /app: the project folder is mounted
# there at runtime and would hide it, and the Windows .venv is unusable here.
# No __pycache__ either: the container would write it into the shared folder,
# where the host Python would pick up files compiled for /app paths.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first, from the lockfile only: this layer is cached
# and only rebuilt when pyproject.toml or uv.lock change.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked
