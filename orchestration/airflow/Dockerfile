FROM apache/airflow:2.2.5-python3.7
ARG AIRFLOW_USER_HOME=/opt/airflow
ARG GCLOUD_VERSION=google-cloud-cli-411.0.0-linux-x86_64.tar.gz
ARG GCLOUD_SERVICE_KEY=/etc/sa.gcpkey.json

COPY ./etc/sa.gcpkey.json ${GCLOUD_SERVICE_KEY}

USER root
RUN sudo chown -R :${AIRFLOW_UID} /opt/

# install gcloud
RUN curl -LO https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GCLOUD_VERSION} && \
    tar xf ${GCLOUD_VERSION} && \
    mv google-cloud-sdk /opt/google-cloud-sdk && \
    ln -s /opt/google-cloud-sdk/bin/* /bin/ && \
    rm ${GCLOUD_VERSION}


USER ${AIRFLOW_UID}
WORKDIR ${AIRFLOW_USER_HOME}

RUN gcloud auth activate-service-account --key-file=${GCLOUD_SERVICE_KEY}

# install airflow deps
COPY orchestration-requirements.txt /opt/requirements.txt
RUN pip3 install --user --no-cache-dir -r /opt/requirements.txt

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []