from dbt_docs import DBTDocs
from flask import Blueprint

from airflow.plugins_manager import AirflowPlugin

bp = Blueprint("plugin", __name__, template_folder="templates", static_folder="static")

v_appbuilder_view = DBTDocs()
v_appbuilder_package = {
    "name": "Docs",
    "category": "DBT",
    "view": v_appbuilder_view,
}


# Defining the plugin class
class AirflowCustomPlugin(AirflowPlugin):
    name = "AirflowCustomPlugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
