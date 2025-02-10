 SELECT *,
        date_trunc(date("{{ ds }}"), month) as calculation_month

FROM `{{ bigquery_export_dataset }}.{{ params.table_name }}`
LIMIT {{ params.volume }}
