FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md ./
COPY src ./src

RUN uv pip install --system .

RUN mkdir -p /data && chmod 700 /data

CMD ["health-sync", "serve"]
