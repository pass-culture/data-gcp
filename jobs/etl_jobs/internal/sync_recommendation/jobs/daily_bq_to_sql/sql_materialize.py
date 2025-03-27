import logging
from datetime import datetime
from pathlib import Path

from services.database import CloudSQLService
from services.materialized_view import MaterializedViewService
from utils.sql_config import MaterializedView

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent.parent / "sql"


class SQLMaterializeOrchestrator:
    """Orchestrator for managing materialized views in CloudSQL."""

    def __init__(self, project_id: str, database_url: str):
        self.project_id = project_id
        self.database_url = database_url
        self.cloudsql_service = CloudSQLService(
            connection_params={"database_url": database_url}
        )
        self.view_service = MaterializedViewService(
            sql_path=SQL_PATH, db_service=self.cloudsql_service
        )

    def refresh_view(self, view: MaterializedView) -> None:
        """Refresh a materialized view.

        Args:
            view: The materialized view to refresh
        """
        logger.info(f"Refreshing materialized view {view.value}")

        try:
            # Generate template variables
            template_vars = {
                "ts_nodash": datetime.now().strftime("%Y%m%d%H%M%S"),
                "view_name": view.value,
            }

            # Refresh view
            self.view_service.refresh_view(view, template_vars)

        except Exception as e:
            logger.error(f"Failed to refresh materialized view {view.value}: {str(e)}")
            raise
