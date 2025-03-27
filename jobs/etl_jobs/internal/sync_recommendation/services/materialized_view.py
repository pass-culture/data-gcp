import logging
from pathlib import Path
from typing import Dict

import jinja2

from services.database import DatabaseService
from utils.sql_config import MaterializedView

logger = logging.getLogger(__name__)


class MaterializedViewService:
    def __init__(self, sql_path: Path, db_service: "DatabaseService"):
        self.sql_path = sql_path
        self.db_service = db_service

    def render_template(self, template_content: str, **kwargs) -> str:
        env = jinja2.Environment(
            autoescape=False,
            undefined=jinja2.StrictUndefined,
        )
        template = env.from_string(template_content)
        return template.render(**kwargs)

    def refresh_view(self, view: MaterializedView, template_vars: Dict) -> None:
        """Refresh a materialized view"""
        logger.info(f"Refreshing materialized view {view.value}")

        try:
            sql_file = self.sql_path / f"create_{view.value}.sql"
            if not sql_file.exists():
                raise ValueError(f"SQL file not found: {sql_file}")

            with open(sql_file) as f:
                sql_template = f.read()

            sql_content = self.render_template(sql_template, **template_vars)
            logger.info(f"Rendered SQL template with variables: {template_vars}")

            self.db_service.execute_query(sql_content)
            logger.info(f"Successfully refreshed materialized view {view.value}")

        except Exception as e:
            logger.error(f"Failed to refresh materialized view {view.value}: {str(e)}")
            raise
