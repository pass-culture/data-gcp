
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.22 /uv /bin/uv

ARG WORKERS=5
ARG GCP_PROJECT=default_project
ARG ENV_SHORT_NAME=default_env

COPY ./pyproject.toml ./pyproject.toml
COPY ./uv.lock ./uv.lock
RUN uv sync --no-dev --no-cache-dir

COPY ./app /app
WORKDIR /

EXPOSE 8080

ENV WORKERS=${WORKERS}
ENV GCP_PROJECT=${GCP_PROJECT}
ENV ENV_SHORT_NAME=${ENV_SHORT_NAME}
CMD uv run --no-dev hypercorn --bind 0.0.0.0:8080 app.app:app --workers=${WORKERS} --timeout 240
