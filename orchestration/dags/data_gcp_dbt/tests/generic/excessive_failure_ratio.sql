{% test excessive_failure_ratio(model, column_name, base_test, max_ratio, kwargs={}) %}

    -- Generate SQL for the base test dynamically
    {% if base_test == "not_null" %}
        {% set failure_query %}
        select {{ column_name }}
        from {{ model }}
        where {{ column_name }} is null
        {% if kwargs.get('where') %}
        and {{ kwargs['where'] }}
        {% endif %}
        {% endset %}

    {% elif base_test == "unique" %}
        {% set failure_query %}
        select {{ column_name }}
        from {{ model }}
        {% if kwargs.get('where') %}
        where {{ kwargs['where'] }}
        {% endif %}
        group by {{ column_name }}
        having count(*) > 1
        {% endset %}

    {% elif base_test == "accepted_values" %}
        {% if "values" not in kwargs %}
            {{
                exceptions.raise_compiler_error(
                    "The 'accepted_values' test requires a 'values' argument."
                )
            }}
        {% endif %}
        {% set values_list = kwargs["values"] | join("', '") %}
        {% set failure_query %}
        select {{ column_name }}
        from {{ model }}
        where {{ column_name }} not in ('{{ values_list }}')
        {% if kwargs.get('where') %}
        and {{ kwargs['where'] }}
        {% endif %}
        {% endset %}

    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported base_test: " ~ base_test) }}
    {% endif %}

    -- Compute failing rows
    with
        failure_table as ({{ failure_query }}),

        failure_count as (select count(*) as failures from failure_table),

        total_count as (
            select count(*) as total
            from {{ model }}
            {% if kwargs.get("where") %} where {{ kwargs["where"] }} {% endif %}
        ),

        failure_ratio_cte as (
            select
                case
                    when total = 0 then 0 else failures * 1.0 / total
                end as failure_ratio
            from failure_count, total_count
        )

    -- Fail if failure ratio exceeds the threshold
    select failure_ratio
    from failure_ratio_cte
    where failure_ratio > {{ max_ratio }}

{% endtest %}
