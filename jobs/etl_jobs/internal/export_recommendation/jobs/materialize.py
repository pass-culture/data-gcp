import logging
from pathlib import Path

from config import MaterializedView
from jobs.import_sql import get_db_connection

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent.parent / "sql"


def refresh_materialized_view(view: MaterializedView) -> None:
    """
    Refresh a materialized view.

    Args:
        view: The materialized view to refresh
    """
    logger.info(f"Refreshing materialized view {view.value}")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create temp view using the SQL file
                sql_file = SQL_PATH / f"create_{view.value}.sql"
                if not sql_file.exists():
                    raise ValueError(f"SQL file not found: {sql_file}")

                with open(sql_file) as f:
                    create_view_sql = f.read()

                # Create temp view
                cursor.execute(f"""
                    CREATE MATERIALIZED VIEW {view.value}_tmp
                    AS {create_view_sql};
                """)

                # Rename views
                cursor.execute(
                    f"ALTER MATERIALIZED VIEW IF EXISTS {view.value} RENAME TO {view.value}_old;"
                )
                cursor.execute(
                    f"ALTER MATERIALIZED VIEW {view.value}_tmp RENAME TO {view.value};"
                )

                # Drop old view
                cursor.execute(
                    f"DROP MATERIALIZED VIEW IF EXISTS {view.value}_old CASCADE;"
                )
                conn.commit()

        logger.info(f"Successfully refreshed materialized view {view.value}")

    except Exception as e:
        logger.error(f"Failed to refresh materialized view {view.value}: {str(e)}")
        raise
