{% for input_table in params.gcp_project_native_env %}

SELECT
    *
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
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
    
{% if not loop.last -%} UNION ALL {%- endif %}
{% endfor %}