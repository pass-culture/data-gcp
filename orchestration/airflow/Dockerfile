FROM apache/airflow:2.6.3-python3.10
ARG GCLOUD_SERVICE_KEY=/etc/sa.gcpkey.json
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-411.0.0-linux-x86_64.tar.gz

# install airflow deps
WORKDIR ${AIRFLOW_USER_HOME}
COPY orchestration-requirements.in /opt/requirements.txt

COPY ./etc/cacert.pem /cacert.pem

ENV REQUESTS_CA_BUNDLE=/cacert.pem
ENV PIP_CERT=/cacert.pem
ENV CURL_CA_BUNDLE=/cacert.pem

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r /opt/requirements.txt

# install gcloud
USER root
RUN apt-get update && apt-get install -y ca-certificates --no-install-recommends && apt-get install git -y
COPY ./etc/sa.gcpkey.json ${GCLOUD_SERVICE_KEY}
RUN curl -LO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} && tar xf ${GCLOUD_VERSION}
ENV PATH $PATH:$AIRFLOW_USER_HOME/google-cloud-sdk/bin
RUN gcloud config set core/custom_ca_certs_file "/cacert.pem"
RUN gcloud auth activate-service-account --key-file=${GCLOUD_SERVICE_KEY}


ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []