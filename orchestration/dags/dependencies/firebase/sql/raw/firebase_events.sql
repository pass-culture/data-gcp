{% for input_table in params.gcp_project_env %}

    select * except (event_date), parse_date('%Y%m%d', event_date) as event_date
    from
        {% if params.dag_type == "intraday" %}
            `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
        {% else %}
            `{{ input_table }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
        {% endif %}
    where
        app_info.id in ("{{ params.app_info_ids | join('", "') }}")
        or app_info.id is null

    {% if not loop.last -%}
        union all
    {%- endif %}
{% endfor %}
