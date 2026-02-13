import json
import os
from pathlib import Path

import yaml
from airflow.security import permissions
from airflow.www.auth import has_access
from dbt_docs import get_airflow_home
from flask import Response, abort, send_from_directory
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose

AIRFLOW_HOME = get_airflow_home()


class DBTLineageViz(AppBuilderBaseView):
    default_view = "home"
    route_base = "/dbtlineageviz"

    STATIC_DIR = f"{AIRFLOW_HOME}/plugins/static/dbt_lineage"
    INDEX_PATH = f"{STATIC_DIR}/index.html"
    CONFIG_YAML_PATH = f"{STATIC_DIR}/config.yaml"

    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def home(self):
        index_file = Path(self.INDEX_PATH)

        if not index_file.is_file():
            print("Error, file not found", os.listdir(AIRFLOW_HOME))
            print("Error, file not found", os.listdir(index_file.parent))
            return abort(404)
        else:
            return index_file.read_text()

    @expose("/data/<path:filename>")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def serve_data(self, filename):
        """Serve static data files. config.json is generated from config.yaml."""
        static_dir = Path(self.STATIC_DIR)

        if not static_dir.is_dir():
            return abort(404)

        # config.json is generated on-the-fly from config.yaml (single source of truth)
        if filename == "config.json":
            return self._serve_config_json()

        allowed_extensions = {".json", ".py", ".js"}
        file_ext = Path(filename).suffix.lower()

        if file_ext not in allowed_extensions:
            return abort(403)

        file_path = static_dir / filename
        if not file_path.is_file():
            return abort(404)

        content_types = {
            ".json": "application/json",
            ".py": "text/plain",
            ".js": "application/javascript",
        }
        return send_from_directory(
            str(static_dir),
            filename,
            mimetype=content_types.get(file_ext, "text/plain"),
        )

    def _serve_config_json(self):
        """Generate config.json from config.yaml (single source of truth)."""
        config_yaml = Path(self.CONFIG_YAML_PATH)
        if not config_yaml.is_file():
            return abort(404)

        with open(config_yaml) as f:
            config = yaml.safe_load(f)

        return Response(
            json.dumps(config, indent=2),
            mimetype="application/json",
        )
