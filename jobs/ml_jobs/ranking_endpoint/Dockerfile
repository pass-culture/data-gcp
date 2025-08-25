
FROM python:3.10

ARG WORKERS=8

COPY ./app/pyproject.toml pyproject.toml
COPY ./app/uv.lock uv.lock
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN uv sync --no-dev --no-cache-dir

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /app
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config="json:custom_logging/config.json"
