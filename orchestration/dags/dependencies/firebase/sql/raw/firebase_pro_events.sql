{% if params.dag_type == "specific_day" and params.custom_day is mapping %}
    {% if params.custom_day is iterable %}
        {% set first_day = yyyymmdd(params.custom_day[0]) %}
        {% set last_day = yyyymmdd(params.custom_day[-1]) %}
        {% set table_id = f"events{params.prefix}{first_day}to{last_day}" %}
    {% else %}
        {% set table_id = f"events{params.prefix}{yyyymmdd(params.custom_day)}" %}
    {% endif %}
{% endif %}

{% for input_table in params.gcp_project_env %}

    select * except (event_date), parse_date('%Y%m%d', event_date) as event_date
    from
        {% if params.dag_type == "intraday" %}
            `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
        {% elif params.dag_type == "daily" %}
            `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
        {% elif params.dag_type == "specific_day" %}
            {% if params.custom_day is iterable %} `{{ input_table }}.{{ table_id }}`
            {% else %}
                `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(params.custom_day) }}`
            {% endif %}
        {% endif %}
    where device.web_info.hostname in ("{{ params.app_info_ids | join('", "') }}")

    {% if not loop.last -%}
        union all
    {%- endif %}
{% endfor %}
