{% test is_valid_booking_status(model, column_name, where_condition=None) %}

    with
        validation as (
            select {{ column_name }} as booking_status
            from {{ model }}
            {% if where_condition is not none %} where {{ where_condition }} {% endif %}
        ),

        errors as (
            select booking_status
            from validation
            where
                booking_status not in ("CANCELLED","CONFIRMED","REIMBURSED","USED")
        )

    select *
    from errors

{% endtest %}
