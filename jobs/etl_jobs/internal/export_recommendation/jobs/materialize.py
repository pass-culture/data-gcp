import logging
from datetime import datetime
from pathlib import Path

import jinja2

from config import MaterializedView
from utils import get_db_connection

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent.parent / "sql"


def render_sql_template(template_content: str, **kwargs) -> str:
    """
    Render a SQL template using Jinja2.

    Args:
        template_content: The SQL template content
        **kwargs: Variables to pass to the template

    Returns:
        The rendered SQL
    """
    # Create a Jinja2 environment with proper settings
    env = jinja2.Environment(
        autoescape=False,  # No HTML escaping for SQL
        undefined=jinja2.StrictUndefined,  # Raise errors for undefined variables
    )

    # Create a template from the content
    template = env.from_string(template_content)

    # Render the template with the provided variables
    return template.render(**kwargs)


def refresh_materialized_view(view: MaterializedView) -> None:
    """
    Refresh a materialized view.

    Args:
        view: The materialized view to refresh
    """
    logger.info(f"Refreshing materialized view {view.value}")

    try:
        # Get SQL file path
        sql_file = SQL_PATH / f"create_{view.value}.sql"
        if not sql_file.exists():
            raise ValueError(f"SQL file not found: {sql_file}")

        # Read SQL file content
        with open(sql_file) as f:
            sql_template = f.read()

        # Generate a timestamp for the template
        now = datetime.now()
        ts_nodash = now.strftime("%Y%m%d%H%M%S")

        # Render the SQL template with variables
        sql_content = render_sql_template(
            sql_template, ts_nodash=ts_nodash, view_name=view.value
        )

        logger.info(f"Rendered SQL template with ts_nodash={ts_nodash}")

        # Connect to the database
        conn = get_db_connection()

        with conn:
            with conn.cursor() as cursor:
                # Execute the SQL script directly
                cursor.execute(sql_content)

                # Commit the transaction
                conn.commit()

                logger.info(f"Successfully refreshed materialized view {view.value}")

    except Exception as e:
        logger.error(f"Failed to refresh materialized view {view.value}: {str(e)}")
        raise
