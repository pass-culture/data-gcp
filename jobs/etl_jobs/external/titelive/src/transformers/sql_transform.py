"""SQL transformation queries for Mode 2 (GCS file load)."""


def get_transform_query(source_table: str, target_table: str) -> str:
    """
    Generate SQL query to transform GCS-loaded data.

    This is a placeholder function that should be customized based on
    the actual structure of the GCS file and desired output schema.

    Args:
        source_table: Full ID of source table (project.dataset.tmp_table)
        target_table: Full ID of target table (project.dataset.target)

    Returns:
        SQL query string for transformation

    Note:
        This query should be customized based on actual data structure.
        The current implementation is a template that assumes the source
        data is already in the correct format and just needs to be inserted.
    """
    query = f"""
        INSERT INTO `{target_table}`
        SELECT
            *
            -- Add transformations here based on actual schema:
            -- CAST(column AS TYPE),
            -- JSON_EXTRACT(column, '$.field') AS new_column,
            -- etc.
        FROM `{source_table}`
        WHERE TRUE
            -- Add filters if needed:
            -- AND column IS NOT NULL
            -- AND date_field >= '2024-01-01'
    """

    return query


def get_create_target_table_query(
    target_table: str, source_table: str | None = None
) -> str:
    """
    Generate SQL query to create target table.

    Args:
        target_table: Full ID of target table (project.dataset.table)
        source_table: Optional source table to copy schema from

    Returns:
        SQL query string for table creation

    Note:
        Customize this based on actual schema requirements.
    """
    if source_table:
        query = f"""
            CREATE TABLE IF NOT EXISTS `{target_table}`
            AS SELECT * FROM `{source_table}` WHERE FALSE
        """
    else:
        # Define schema explicitly
        query = f"""
            CREATE TABLE IF NOT EXISTS `{target_table}` (
                id STRING,
                -- Add other columns based on expected schema:
                -- titre STRING,
                -- auteurs_multi STRING,
                -- article_ean STRING,
                -- article_prix FLOAT64,
                -- article_datemodification DATE,
                -- etc.
            )
        """

    return query
