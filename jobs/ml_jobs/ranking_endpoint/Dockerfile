
FROM python:3.10
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ARG WORKERS=8

COPY ./app/pyproject.toml pyproject.toml
COPY ./app/uv.lock uv.lock
RUN uv venv --python 3.10
RUN uv sync

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /app
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config="json:custom_logging/config.json"
