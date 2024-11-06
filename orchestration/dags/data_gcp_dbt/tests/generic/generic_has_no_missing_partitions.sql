{% test generic_has_no_missing_partitions(model, column_name, where_condition=null) %}

    {% set missing_partitions = get_missing_date_partitions(
        model, date_column_name=column_name, where_condition=where_condition
    ) %}

    {% if missing_partitions | length > 0 %}
        with
            missing_dates as (
                select missing_partition_date, existing_partition_date
                from
                    unnest(
                        [
                            {% for partition in missing_partitions %}
                                struct(
                                    "{{ partition['missing_partition_date'] }}"
                                    as missing_partition_date,
                                    "{{ partition['existing_partition_date'] }}"
                                    as existing_partition_date
                                )
                                {% if not loop.last %}, {% endif %}
                            {% endfor %}
                        ]
                    )
            )
        select *
        from missing_dates
    {% else %} select {{ column_name }} from {{ model }} where true = false
    {% endif %}

{% endtest %}
