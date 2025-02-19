{% test generic_has_no_missing_partitions(model, column_name, where_condition=null) %}
    -- Exclude from CI as this test Tables while CI only supports Views
    {{
        config(
            enabled=target.profile_name != "CI"
            and env_var("IS_CI", "false") == "false"
        )
    }}

    {% set missing_partitions = get_missing_date_partitions(
        model, date_column_name=column_name, where_condition=where_condition
    ) %}

    {% if missing_partitions | length > 0 %}
        with
            missing_dates as (
                select missing_partition_date
                from
                    unnest(
                        [
                            {% for partition in missing_partitions %}
                                struct(
                                    "{{ partition['missing_partition_date'] }}"
                                    as missing_partition_date
                                )
                                {% if not loop.last %}, {% endif %}
                            {% endfor %}
                        ]
                    )
            )
        select *
        from missing_dates
        order by missing_partition_date desc
    {% else %} select {{ column_name }} from {{ model }} where true = false
    {% endif %}

{% endtest %}
