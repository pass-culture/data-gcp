
FROM python:3.10
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ARG WORKERS=8

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /app
RUN uv venv --python 3.10
RUN uv sync
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config="json:custom_logging/config.json"
