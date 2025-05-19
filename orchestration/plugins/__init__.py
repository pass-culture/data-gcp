from flask import Blueprint

from airflow.plugins_manager import AirflowPlugin


def init_plugin():
    try:
        from dbt_docs import DBTDocs

        bp = Blueprint(
            "plugin", __name__, template_folder="templates", static_folder="static"
        )

        v_appbuilder_view = DBTDocs()
        v_appbuilder_package = {
            "name": "Docs",
            "category": "DBT",
            "view": v_appbuilder_view,
        }

        return bp, v_appbuilder_package
        # Defining the plugin class

    except Exception as e:
        print(e)
        return None, None


bp, v_appbuilder_package = init_plugin()


class AirflowCustomPlugin(AirflowPlugin):
    name = "AirflowCustomPlugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
