
FROM astral/uv:0.11-python3.10-trixie-slim
ARG WORKERS=8

# Create non-root user early so COPY --chown can reference it
RUN groupadd --system --gid 999 appuser \
    && useradd --system --gid 999 --uid 999 appuser

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1
COPY --chown=appuser:appuser ./app/pyproject.toml ./app/uv.lock /app/
WORKDIR /app
RUN uv venv --python 3.10 && uv sync && chown -R appuser:appuser /app/.venv

COPY --chown=appuser:appuser ./app /app
COPY --chown=appuser:appuser ./metadata /app/metadata

EXPOSE 8080
ENV WORKERS=${WORKERS}
USER appuser

ENTRYPOINT ["sh", "-c", "exec uv run hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config=json:custom_logging/config.json"]
