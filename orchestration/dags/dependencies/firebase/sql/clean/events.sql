SELECT
    *
FROM
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}`
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
AND 
    {% if params.dag_type == 'intraday' %}
        event_date = DATE("{{ ds }}")
    {% else %}
        event_date = DATE("{{ add_days(ds, -1) }}")
    {% endif %}
    
