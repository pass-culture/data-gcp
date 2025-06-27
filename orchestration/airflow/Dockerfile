# Docker Env variables
ARG NETWORK_MODE=default

################################
## Stage 1 Builder with proxy ##
FROM apache/airflow:2.9.3-python3.10 AS base-proxy
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.pem
COPY airflow/etc/nscacert_combined.pem ${CERT_PATH_DOCKER}
ENV REQUESTS_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV SSL_CERT_FILE=${CERT_PATH_DOCKER}
ENV CURL_CA_BUNDLE=${CERT_PATH_DOCKER}
ENV HTTPLIB2_CA_CERTS=${CERT_PATH_DOCKER}

##################################
## Stage 1 Builder without proxy ##
FROM apache/airflow:2.9.3-python3.10 AS base-default

#####################
## Stage 2 Builder ##
FROM base-${NETWORK_MODE} AS general-builder
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-411.0.0-linux-x86_64.tar.gz
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.pem
COPY --from=base-proxy ${CERT_PATH_DOCKER} ${CERT_PATH_DOCKER}

# Install airflow deps
WORKDIR ${AIRFLOW_USER_HOME}
COPY ./airflow/orchestration-requirements.txt ./requirements.txt
RUN uv pip install -r requirements.txt

# Install gcloud
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates curl \
    && update-ca-certificates \
    && curl -fsSL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} \
         -o /tmp/${GCLOUD_VERSION} \
    && cd /tmp \
    && tar -xzf ${GCLOUD_VERSION} \
    && ./google-cloud-sdk/install.sh --quiet \
         --usage-reporting=false \
         --path-update=false \
    && mv /tmp/google-cloud-sdk ${AIRFLOW_USER_HOME}/google-cloud-sdk \
    && chown -R airflow ${AIRFLOW_USER_HOME}/google-cloud-sdk \
    && rm /tmp/${GCLOUD_VERSION}

ENV PATH="${PATH}:${AIRFLOW_USER_HOME}/google-cloud-sdk/bin"

# Silence gcloud logs for Paramiko ProxyCommand usage
RUN gcloud config set core/verbosity error \
    && gcloud config set core/log_http false

# Switch back to airflow user for subsequent stages
USER airflow

################################
## Stage 3 Builder with proxy ##
FROM general-builder AS builder-proxy
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.pem
COPY --from=base-proxy ${CERT_PATH_DOCKER} ${CERT_PATH_DOCKER}
RUN gcloud config set core/custom_ca_certs_file ${CERT_PATH_DOCKER}

################################
## Stage 3 Builder without proxy ##
FROM general-builder AS builder-default

###################
## Final builder ##
FROM builder-${NETWORK_MODE} AS final-builder
ARG CERT_PATH_DOCKER=/usr/local/share/ca-certificates/nscacert_combined.pem

# Copy sync script and pools config as root to allow chmod
USER root
COPY airflow/scripts/sync_pools.py sync_pools.py
COPY airflow/config/pools.json pools.json
RUN chmod +x sync_pools.py

# Switch back to airflow user
USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
