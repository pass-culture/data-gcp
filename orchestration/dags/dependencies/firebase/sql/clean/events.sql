SELECT
    *
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}

    
