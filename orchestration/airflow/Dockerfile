# =====================================================================
# GLOBAL SCOPE DEFINITIONS
# All ARGs used in any FROM clause MUST be declared here at the top.
# =====================================================================
ARG UV_VERSION=0.11.14
ARG NETWORK_MODE=default

# Fetch uv binary template stage
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv_source

################################
## Stage 1 Builder with proxy ##
FROM apache/airflow:slim-2.11.2-python3.12 AS base-proxy
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.crt
COPY orchestration/airflow/etc/nscacert_combined.pem ${CERT_PATH_DOCKER}
ENV REQUESTS_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV SSL_CERT_FILE=${CERT_PATH_DOCKER}
ENV CURL_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV HTTPLIB2_CA_CERTS=${CERT_PATH_DOCKER}

##################################
## Stage 1 Builder without proxy ##
FROM apache/airflow:slim-2.11.2-python3.12 AS base-default

#####################
## Stage 2 Builder ##
# Inherits NETWORK_MODE cleanly from global scope
FROM base-${NETWORK_MODE} AS general-builder
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-538.0.0-linux-x86_64.tar.gz
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.crt

USER root

# 1. Copy over the proxy cert if available and establish security trust
COPY --from=base-proxy ${CERT_PATH_DOCKER} ${CERT_PATH_DOCKER}
RUN chmod 644 ${CERT_PATH_DOCKER}

# 2. System Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    tar \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 3. Grab uv binary via the dynamic global stage
COPY --from=uv_source /uv /bin/uv

# Global UV settings optimized for performance
ENV UV_CACHE_DIR=/tmp/.cache/uv
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Proxy-specific target flags for UV
ENV UV_CERT_BUNDLE=${CERT_PATH_DOCKER}
ENV UV_NATIVE_TLS=true
ENV SSL_CERT_FILE=${CERT_PATH_DOCKER}
ENV REQUESTS_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV PIP_CERT=${CERT_PATH_DOCKER}
ENV CURL_CA_BUNDLE=${CERT_PATH_DOCKER}

# 4. Streamlined Gcloud installation using production direct tar method
RUN curl --proto '=https' --proto-redir '=https' -fsSL \
        https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} \
        -o /tmp/${GCLOUD_VERSION} \
    && tar -xf /tmp/${GCLOUD_VERSION} -C ${AIRFLOW_USER_HOME} \
    && ${AIRFLOW_USER_HOME}/google-cloud-sdk/install.sh --quiet --usage-reporting=false --path-update=false \
    && chown -R airflow ${AIRFLOW_USER_HOME}/google-cloud-sdk \
    && rm /tmp/${GCLOUD_VERSION}

ENV PATH="${AIRFLOW_USER_HOME}/.venv/bin:${AIRFLOW_USER_HOME}/google-cloud-sdk/bin:$PATH"
ENV USE_GKE_GCLOUD_AUTH_PLUGIN=True

# 5. Configure Gcloud SDK
RUN gcloud config set core/custom_ca_certs_file ${CERT_PATH_DOCKER} \
    && gcloud components install gke-gcloud-auth-plugin --quiet \
    && gcloud config set core/verbosity error \
    && gcloud config set core/log_http false

# 6. Install Python Dependencies via uv
WORKDIR ${AIRFLOW_USER_HOME}
COPY --chown=airflow:root uv.lock pyproject.toml ./

USER airflow
RUN uv sync --frozen --no-dev

################################
## Stage 3 Builder with proxy ##
FROM general-builder AS builder-proxy

################################
## Stage 3 Builder without proxy ##
FROM general-builder AS builder-default

###################
## Final builder ##
# Inherits NETWORK_MODE cleanly from global scope without stage boundary blocks
FROM builder-${NETWORK_MODE} AS final-builder

ARG AIRFLOW_USER_HOME=/opt/airflow

# Copy sync script and pools config as root to allow chmod
USER root

COPY orchestration/airflow/scripts/sync_pools.py ${AIRFLOW_USER_HOME}/sync_pools.py
COPY orchestration/airflow/config/pools.json ${AIRFLOW_USER_HOME}/pools.json
RUN chmod +x ${AIRFLOW_USER_HOME}/sync_pools.py

# Switch back to airflow user for container execution context
USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []