{% test not_null_multiple_columns(model, columns) %}

    {% if not columns %}
        {{
            exceptions.raise_compiler_error(
                "The 'columns' argument is required for the not_null_multiple_columns test."
            )
        }}
    {% endif %}

    {% set conditions = [] %}
    {% for column in columns %}
        {% do conditions.append(column ~ " IS NULL") %}
    {% endfor %}

    with
        violations as (
            select count(*) as num_violations
            from {{ model }}
            where {{ conditions | join(" OR ") }}
        )
    select *
    from violations
    where num_violations > 0

{% endtest %}
