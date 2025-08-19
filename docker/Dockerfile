ARG PYTHON_BUILD_CACHE_VERSION=latest

FROM ghcr.io/jr200/build-cache-python:${PYTHON_BUILD_CACHE_VERSION} AS builder

COPY --chown=app_user:app_user pyproject.toml poetry.lock* ./
RUN poetry install --only main --no-root -vv
COPY --chown=app_user:app_user ./src /app/src
RUN poetry install --only main -vv


FROM python:3.12-slim-bookworm

LABEL maintainer "jr200 <jr200@users.noreply.github.com>"
LABEL org.opencontainers.image.source https://github.com/jr200/nats-s3-monitor

RUN useradd -u 1000 -d /app -m -s /bin/bash app_user
WORKDIR /app

ENV PATH="/app/.venv/bin:/app/.local/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER app_user

COPY --from=builder --chown=app_user:app_user /app/src /app/src
COPY --from=builder --chown=app_user:app_user /app/.venv /app/.venv

ENTRYPOINT ["start_api"]
