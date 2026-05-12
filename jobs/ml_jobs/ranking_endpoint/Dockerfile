
FROM astral/uv:0.11-python3.10-trixie
ARG WORKERS=8

# Create non-root user early so COPY --chown can reference it
RUN groupadd --system --gid 999 appuser \
    && useradd --system --gid 999 --uid 999 appuser

# Allow uv to compile bytecode (faster init) and disable caching to reduce image size
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1
COPY --chown=appuser:appuser ./app/pyproject.toml ./app/uv.lock /app/
WORKDIR /app
RUN uv venv --python 3.10 && uv sync && chown -R appuser:appuser /app/.venv

# Copy application code and metadata and set ownership to non-root user
COPY --chown=appuser:appuser ./app /app
COPY --chown=appuser:appuser ./metadata /app/metadata

# Expose the application port and set environment variable for workers
EXPOSE 8080
ENV WORKERS=${WORKERS}

# Switch to non-root user for security
USER appuser

# Use uv to run the app
ENTRYPOINT ["sh", "-c", "exec uv run hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS} --log-config=json:custom_logging/config.json"]
