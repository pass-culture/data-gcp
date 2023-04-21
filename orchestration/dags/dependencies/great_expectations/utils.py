import pandas as pd


def get_table_volume_bounds(partition_field, dataset_name, table_name, nb_days):
    """This function returns the daily confiance interval
    for a table volume (with level of 10%).
    """

    query = f"""
    WITH daily_volume as (
        SELECT { partition_field }, count(*) as volume
        FROM `{ dataset_name }.{ table_name }` 
        WHERE { partition_field } between current_date() - {nb_days} - 1 and current_date() - 1
        GROUP BY 1
    )
    select avg(volume) as avg_volume
    from daily_volume
    """

    df = pd.read_gbq(query)

    return [df["avg_volume"][0] * 0.9, df["avg_volume"][0] * 1.1]


def get_date_fields(dataset_name, table_name):
    """Function to get the names of date fields from a bigquery table."""

    query = f"""
    SELECT
      table_catalog
      , table_schema
      , table_name
      , array_agg(column_name) as date_fields
    FROM `{dataset_name}.INFORMATION_SCHEMA.COLUMNS`
    where data_type in ('DATETIME', 'DATE', 'TIMESTAMP')
    and table_name = "{table_name}"
    group by 1,2,3
    """
    df = pd.read_gbq(query)
    date_fields = list(df["date_fields"][0])

    return date_fields
