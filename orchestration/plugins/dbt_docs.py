import os
from pathlib import Path

from airflow.exceptions import AirflowConfigException
from airflow.security import permissions
from airflow.www.auth import has_access
from flask import abort
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose


def get_airflow_home() -> str:
    if os.environ.get("LOCAL_ENV", None) == "1":
        return "/opt/airflow/"
    if os.environ.get("DAG_FOLDER", None) == "/opt/airflow/dags":
        return "/opt/airflow/"
    if os.environ.get("DAG_FOLDER", None) == "/home/airflow/gcs/dags":
        return "/home/airflow/gcs"
    raise AirflowConfigException(
        "Airflow home not found, failed to determine environment"
    )


AIRFLOW_HOME = get_airflow_home()


class DBTDocs(AppBuilderBaseView):
    default_view = "home"
    STATIC_PATH = f"{AIRFLOW_HOME}/plugins/static/dbt_docs/index.html"

    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def home(self):
        dbt_docs_dir = Path(self.STATIC_PATH)

        if not dbt_docs_dir.is_file():
            print("Error, file not found", os.listdir(AIRFLOW_HOME))
            print("Error, file not found", os.listdir(self.STATIC_PATH.parent))
            return abort(404)
        else:
            return dbt_docs_dir.read_text()


class DBTColibriDocs(AppBuilderBaseView):
    default_view = "home"
    STATIC_PATH = f"{AIRFLOW_HOME}/plugins/static/dbt_docs/colibri.html"

    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def home(self):
        dbt_docs_dir = Path(self.STATIC_PATH)

        if not dbt_docs_dir.is_file():
            print("Error, file not found", os.listdir(AIRFLOW_HOME))
            print("Error, file not found", os.listdir(self.STATIC_PATH.parent))
            return abort(404)
        else:
            return dbt_docs_dir.read_text()
