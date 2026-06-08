# Image for the docker-compose distributed stack. Used by BOTH the worker service
# (runs the long-lived worker loop) and the test service (runs pytest inside the
# VM so it shares the SAME kernel + named volume as the workers — sqlite WAL works
# there, unlike a macOS bind mount over virtiofs). Build context = the repo root.
FROM python:3.13-slim

RUN pip install --no-cache-dir uv
WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src ./src
COPY test ./test
COPY deploy ./deploy

# setuptools-scm needs a git repo to detect the version; supply a fallback so the
# Docker build (which has no .git) succeeds.
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# runtime + the redis/postgres/rqlite/mongo/surrealdb/sqs extras (the backend
# clients) + dev group (pytest, for the test service). One image keeps the stack
# simple; it's a dev/integration stack.
RUN uv sync --frozen --extra redis --extra postgres --extra rqlite --extra mongo --extra surrealdb --extra sqs --extra dynamodb

# demo_actions resolves from deploy/ at worker runtime; the registry is baked in
ENV PYTHONPATH=/app/deploy
ENV STM_DEFINITIONS_DIR=/app/deploy/definitions

CMD ["/app/.venv/bin/python", "-u", "-m", "harel.worker"]
