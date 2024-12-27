import os
from pathlib import Path

from flask import abort
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

from airflow.security import permissions
from airflow.www.auth import has_access


def get_airflow_home() -> str:
    if os.environ.get("LOCAL_ENV", None) == "1":
        return "/opt/airflow/"
    return "/home/airflow/gcs"


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
