# Docker Env variables
ARG NETWORK_MODE=default

################################
## Stage 1 Builder with proxy ##
FROM apache/airflow:slim-2.11.2-python3.12 AS base-proxy
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.crt
COPY orchestration/airflow/etc/nscacert_combined.pem ${CERT_PATH_DOCKER}
ENV REQUESTS_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV SSL_CERT_FILE=${CERT_PATH_DOCKER}
ENV CURL_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV HTTPLIB2_CA_CERTS=${CERT_PATH_DOCKER}
ENV CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE=${CERT_PATH_DOCKER}
ENV UV_CERT_BUNDLE=${CERT_PATH_DOCKER}
ENV UV_NATIVE_TLS=true
ENV PIP_CERT=${CERT_PATH_DOCKER}


##################################
## Stage 1 Builder without proxy ##
FROM apache/airflow:slim-2.11.2-python3.12 AS base-default

#####################
## Stage 2 Builder ##
FROM base-${NETWORK_MODE} AS general-builder
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-538.0.0-linux-x86_64.tar.gz
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.crt

# 1. COPY CERT as root and set permissions so both root and airflow users can read it
USER root
COPY --from=base-proxy ${CERT_PATH_DOCKER} ${CERT_PATH_DOCKER}
RUN chmod 644 ${CERT_PATH_DOCKER}

# 2. INSTALL SYSTEM DEPS & TRUST CERT
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates curl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 3. INSTALL UV
COPY --from=ghcr.io/astral-sh/uv:0.11.15 /uv /bin/

# 4. INSTALL GCLOUD
RUN curl --proto '=https' --proto-redir '=https' -fsSL \
    https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} \
    -o /tmp/${GCLOUD_VERSION} \
    && cd /tmp && tar -xzf ${GCLOUD_VERSION} \
    && ./google-cloud-sdk/install.sh --quiet --usage-reporting=false --path-update=false \
    && mv /tmp/google-cloud-sdk ${AIRFLOW_USER_HOME}/google-cloud-sdk \
    && chown -R airflow ${AIRFLOW_USER_HOME}/google-cloud-sdk \
    && rm /tmp/${GCLOUD_VERSION}

ENV PATH="${PATH}:${AIRFLOW_USER_HOME}/google-cloud-sdk/bin"
ENV USE_GKE_GCLOUD_AUTH_PLUGIN=True

# 5. CONFIGURE GCLOUD & INSTALL PLUGIN
RUN gcloud components install gke-gcloud-auth-plugin --quiet \
    && gcloud config set core/verbosity error \
    && gcloud config set core/log_http false

# 6. INSTALL PYTHON DEPS
USER root
RUN chown -R airflow:root /home/airflow/.local
USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
COPY uv.lock pyproject.toml ./

ENV PATH="${AIRFLOW_USER_HOME}/.venv/bin:$PATH"

# Force UV_CERT_BUNDLE again at the command level to ensure 'airflow' user sees it
RUN uv sync --frozen --only-group airflow

################################
## Stage 3 Builder with proxy ##
FROM general-builder AS builder-proxy
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.crt

RUN gcloud config set core/custom_ca_certs_file ${CERT_PATH_DOCKER}

################################
## Stage 3 Builder without proxy ##
FROM general-builder AS builder-default

###################
## Final builder ##
FROM builder-${NETWORK_MODE} AS final-builder

# Copy sync script and pools config as root to allow chmod
USER root
COPY orchestration/airflow/scripts/sync_pools.py sync_pools.py
COPY orchestration/airflow/config/pools.json pools.json
RUN chmod +x sync_pools.py

# Switch back to airflow user
USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
