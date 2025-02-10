select *, date_trunc(date("{{ ds }}"), month) as calculation_month

from `{{ bigquery_export_dataset }}.{{ params.table_name }}`
limit {{ params.volume }}
