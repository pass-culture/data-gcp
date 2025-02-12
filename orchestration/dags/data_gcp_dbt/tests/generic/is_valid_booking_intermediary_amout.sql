{% test is_valid_booking_intermediary_amout(
    model, column_name, max_amout=300, where_condition=None
) %}

    with
        validation as (
            select {{ column_name }} as amount_field
            from {{ model }}
            {% if where_condition is not none %} where {{ where_condition }} {% endif %}
        ),

        errors as (
            select amount_field
            from validation
            where amount_field is null or amount_field > {{ max_amout }}
        )

    select *
    from errors

{% endtest %}
