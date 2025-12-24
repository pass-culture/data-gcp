from airflow.plugins_manager import AirflowPlugin
from dbt_docs import DBTColibriDocs, DBTDocs
from flask import Blueprint

bp = Blueprint("plugin", __name__, template_folder="templates", static_folder="static")

v_appbuilder_dbt_docs_view = DBTDocs()
v_appbuilder_dbt_docs_package = {
    "name": "DBT Docs",
    "category": "DBT",
    "view": v_appbuilder_dbt_docs_view,
}

v_appbuilder_colibri_view = DBTColibriDocs()
v_appbuilder_colibri_package = {
    "name": "Colibri Docs",
    "category": "DBT",
    "view": v_appbuilder_colibri_view,
}


# Defining the plugin class
class AirflowCustomPlugin(AirflowPlugin):
    name = "AirflowCustomPlugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_dbt_docs_package, v_appbuilder_colibri_package]
