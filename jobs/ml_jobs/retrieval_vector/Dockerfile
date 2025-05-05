
FROM python:3.10-slim
COPY --from=ghcr.io/astral-sh/uv:0.4.23 /uv /bin/uv

ARG WORKERS=5


COPY ./pyproject.toml ./pyproject.toml
COPY ./uv.lock ./uv.lock
RUN uv sync --no-dev --no-cache-dir

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /

EXPOSE 8080

ENV WORKERS=${WORKERS}

CMD uv run --no-dev hypercorn --bind 0.0.0.0:8080 app.app:app --workers=${WORKERS} --log-config="json:app/logging/config.json"
