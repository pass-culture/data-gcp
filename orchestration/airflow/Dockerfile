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

# Install airflow deps
WORKDIR ${AIRFLOW_USER_HOME}
COPY ./airflow/orchestration-requirements.txt ./requirements.txt
RUN uv pip install -r requirements.txt
RUN uv pip install astronomer-cosmos


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

# Install gcloud
USER root
RUN apt-get update && apt-get install -y git ca-certificates --no-install-recommends && update-ca-certificates
RUN curl -LO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} && tar xf ${GCLOUD_VERSION}
ENV PATH=$PATH:$AIRFLOW_USER_HOME/google-cloud-sdk/bin


################################
## Stage 3 Builder with proxy ##
FROM general-builder AS builder-proxy
RUN gcloud config set core/custom_ca_certs_file ${CERT_PATH_DOCKER}

################################
## Stage 3 Builder without proxy ##
FROM general-builder AS builder-default

###################
## Final builder ##
FROM builder-${NETWORK_MODE} AS final-builder
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
