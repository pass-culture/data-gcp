FROM apache/airflow:2.2.5-python3.7
ARG AIRFLOW_USER_HOME=/opt/airflow

USER ${AIRFLOW_UID}
WORKDIR ${AIRFLOW_USER_HOME}

COPY orchestration-requirements.txt /opt/requirements.txt
RUN pip3 install --user --no-cache-dir -r /opt/requirements.txt

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD []