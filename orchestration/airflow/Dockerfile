FROM apache/airflow:2.9.1-python3.10
ARG GCLOUD_SERVICE_KEY=/etc/sa.gcpkey.json
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-411.0.0-linux-x86_64.tar.gz

# install airflow deps
WORKDIR ${AIRFLOW_USER_HOME}
COPY airflow/orchestration-requirements.txt /opt/requirements.txt
RUN uv pip install --no-cache-dir -r /opt/requirements.txt

# Install DBT dependencies
COPY dags/data_gcp_dbt/dbt-requirements.txt /opt/dbt-requirements.txt
RUN uv pip install --no-cache-dir -r /opt/dbt-requirements.txt

# install gcloud
USER root
RUN apt-get update && apt-get install -y git ca-certificates --no-install-recommends
COPY ./airflow/etc/sa.gcpkey.json ${GCLOUD_SERVICE_KEY}
RUN curl -LO -k https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} && tar xf ${GCLOUD_VERSION}
ENV PATH=$PATH:$AIRFLOW_USER_HOME/google-cloud-sdk/bin
RUN gcloud auth activate-service-account --key-file=${GCLOUD_SERVICE_KEY}


ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []
