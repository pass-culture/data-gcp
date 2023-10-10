FROM apache/airflow:2.4.3-python3.10
ARG GCLOUD_SERVICE_KEY=/etc/sa.gcpkey.json
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-411.0.0-linux-x86_64.tar.gz

# install airflow deps
WORKDIR ${AIRFLOW_USER_HOME}
COPY orchestration-requirements.txt /opt/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r /opt/requirements.txt

# install gcloud
USER root
RUN apt-get update && apt-get install -y ca-certificates --no-install-recommends
COPY ./etc/sa.gcpkey.json ${GCLOUD_SERVICE_KEY}
RUN curl -LO -k https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} && tar xf ${GCLOUD_VERSION}
ENV PATH $PATH:$AIRFLOW_USER_HOME/google-cloud-sdk/bin
RUN gcloud auth activate-service-account --key-file=${GCLOUD_SERVICE_KEY}


ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []