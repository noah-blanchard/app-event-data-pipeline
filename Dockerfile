# Pipeline runner: Python 3.12, Java 17 (required by Spark) and uv.
# Bookworm (Debian 12) is pinned because it still ships OpenJDK 17.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

# Keep the virtual environment outside /app: the project folder is mounted
# there at runtime and would hide it, and the Windows .venv is unusable here.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install dependencies first, from the lockfile only: this layer is cached
# and only rebuilt when pyproject.toml or uv.lock change.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked
