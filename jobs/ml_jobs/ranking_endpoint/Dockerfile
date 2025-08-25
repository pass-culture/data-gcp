
FROM ghcr.io/astral-sh/uv:python3.10-alpine

ARG WORKERS=8
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

COPY ./app/pyproject.toml pyproject.toml
COPY ./app/uv.lock uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /app
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config="json:custom_logging/config.json"
