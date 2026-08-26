# Phase 4 worker image.
#
# Note: ingestion/ and load_balancer/ are still run locally via uvicorn
# (no Dockerfile yet) as of Phase 3 -- containerizing them is separate,
# pre-existing scope, not something Phase 4 needs to solve. The worker
# needs a container now because docker-compose.yml's "run multiple
# workers" checklist item (section 9, Phase 4) is easiest to verify via
# `docker compose up --scale worker=N`.

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ./ingestion/
COPY worker/ ./worker/

CMD ["arq", "worker.consumer.WorkerSettings"]
