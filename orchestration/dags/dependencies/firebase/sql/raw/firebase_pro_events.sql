{% for input_table in params.gcp_project_native_env %}

SELECT
    * except(event_date), 
    PARSE_DATE('%Y%m%d', event_date) as event_date
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}
WHERE
    device.web_info.hostname IN (
        "{{ params.app_info_ids | join('", "') }}"
    )
    
{% if not loop.last -%} UNION ALL {%- endif %}
{% endfor %}