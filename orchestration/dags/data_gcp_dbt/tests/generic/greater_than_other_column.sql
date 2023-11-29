{% test greater_than_other_column(model, column_name, other_column) %}

with validation as (

    select
        {{ column_name }} as column_name
        , {{ other_column }} as other_column

    from {{ model }}

),

validation_errors as (

    select
        *
    from validation
    where column_name < other_column
)

select *
from validation_errors

{% endtest %}