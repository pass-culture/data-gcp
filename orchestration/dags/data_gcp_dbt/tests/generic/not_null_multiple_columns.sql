{% test not_null_multiple_columns(model, columns) %}

    {{
        config(severity='ERROR')
    }}

    {% if not columns %}
        {{ exceptions.raise_compiler_error("The 'columns' argument is required for the not_null_multiple_columns test.") }}
    {% endif %}

    {% set conditions = [] %}
    {% for column in columns %}
        {% do conditions.append(column ~ " IS NULL") %}
    {% endfor %}

    with violations as (SELECT
        COUNT(*) AS num_violations
    FROM {{ model }}
    WHERE {{ conditions | join(' OR ') }}
    )
    select * from violations
    where num_violations > 0

{% endtest %}
