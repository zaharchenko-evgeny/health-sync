FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREFECT_HOME=/data/prefect \
    PREFECT_SERVER_ANALYTICS_ENABLED=false \
    PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY=false \
    PREFECT_TELEMETRY_ENABLE_RESOURCE_METRICS=false

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md ./
COPY src ./src

RUN uv pip install --system .

RUN mkdir -p /data/prefect /root/.prefect && chmod 700 /data /data/prefect /root/.prefect

CMD ["health-sync", "serve"]
