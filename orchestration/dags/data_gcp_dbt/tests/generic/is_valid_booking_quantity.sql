{% test is_valid_booking_quantity(model, column_name, where_condition=None) %}

    with
        validation as (
            select {{ column_name }} as booking_quantity
            from {{ model }}
            {% if where_condition is not none %} where {{ where_condition }} {% endif %}
        ),

        errors as (
            select booking_quantity
            from validation
            where
                booking_quantity not in (1,2)
        )

    select *
    from errors

{% endtest %}
