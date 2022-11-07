SELECT
    *
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}
WHERE
    {% if params.table_type == 'pro' %}
        device.web_info.hostname IN (
            "{{ params.app_info_ids | join('", "') }}"
        )
    {% else %}
        app_info.id IN (
            "{{ params.app_info_ids | join('", "') }}"
        )
        OR app_info.id is NULL
    {% endif %}
    
