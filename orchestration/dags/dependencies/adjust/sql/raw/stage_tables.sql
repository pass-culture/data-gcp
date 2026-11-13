create or replace table `{{ staging_table_id }}`
options (
    expiration_timestamp = timestamp_add(current_timestamp(), interval 24 hour)
) as
{% for table_name in table_names %}
    select
    {% for column_name in expected_schema %}
        {% if column_name == "day" %}
                date(day) as day,
        {% else %}
            {{ column_name }},
        {% endif %}
    {% endfor %}
        @source_table_{{ loop.index0 }} as source_table_name,
        current_timestamp() as loaded_at
    from `{{ source_project }}.{{ source_dataset }}.{{ table_name }}`
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
