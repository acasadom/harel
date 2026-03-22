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

# runtime + the redis/postgres extras (RedisTransport/RedisStore/PostgresStore
# clients) + dev group (pytest, for the test service). One image keeps the stack
# simple; it's a dev/integration stack.
RUN uv sync --frozen --extra redis --extra postgres --extra rqlite --extra sqs

# demo_actions resolves from deploy/ at worker runtime; the registry is baked in
ENV PYTHONPATH=/app/deploy
ENV STM_DEFINITIONS_DIR=/app/deploy/definitions

CMD ["/app/.venv/bin/python", "-u", "-m", "harel.worker"]
