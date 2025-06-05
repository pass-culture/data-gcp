
FROM python:3.10
COPY --from=ghcr.io/astral-sh/uv:0.4.23 /uv /bin/uv

ARG WORKERS=8

COPY ./api-requirements.txt ./
RUN uv pip install --no-cache-dir -r api-requirements.txt --system

COPY ./app /app
COPY ./metadata /app/metadata
WORKDIR /app

EXPOSE 8080

ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config="json:custom_logging/config.json"
