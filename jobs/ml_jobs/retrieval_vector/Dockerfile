
FROM python:3.10-slim
COPY --from=ghcr.io/astral-sh/uv:0.4.18 /uv /bin/uv

ARG WORKERS=5


COPY ./api-requirements.txt ./
RUN uv pip install --no-cache-dir -r api-requirements.txt --system

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /

EXPOSE 8080

ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app.app:app --workers=${WORKERS} --log-config="json:app/logging/config.json"
