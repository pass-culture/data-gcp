{% test is_valid_beneficiary_birth_date(
    model, column_name, where_condition=None
) %}

    with
        validation as (
            select {{ column_name }} as birth_date
            from {{ model }}
            {% if where_condition is not none %} where {{ where_condition }} {% endif %}
        ),

        errors as (
            select birth_date
            from validation
            where birth_date not between DATE("2001-05-01") AND DATE_SUB(current_date, interval 15 year)
        )

    select *
    from errors

{% endtest %}
