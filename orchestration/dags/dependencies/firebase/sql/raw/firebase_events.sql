{% for input_table in params.gcp_project_env %}

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
    app_info.id IN (
        "{{ params.app_info_ids | join('", "') }}"
    )
    OR app_info.id is NULL

{% if not loop.last -%} UNION ALL {%- endif %}
{% endfor %}
