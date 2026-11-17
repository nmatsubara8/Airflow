# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build deps
RUN pip install --no-cache-dir hatchling

# Copy only what's needed for the install
COPY pyproject.toml ./
COPY src/ ./src/

# Build wheel and install into /install prefix
RUN pip install --no-cache-dir --prefix=/install .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages
COPY --from=builder /install /usr/local

# Copy config and runtime files (not source — already installed as package)
COPY .airflow/ ./.airflow/

# Default env — override via docker-compose / environment:
ENV OLLAMA_STUB=false \
    AIRFLOW_TASKBOARD_DIR=/data/tasks \
    AIRFLOW_EXECUTION_LOG=/data/execution.jsonl \
    PYTHONUNBUFFERED=1

# Persistent state volume
VOLUME ["/data"]

# CMD is overridden per service in docker-compose.yml
CMD ["python", "-m", "airflow_mcp", "taskboard"]
