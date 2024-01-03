{% test sum_2_columns_equals_another_column(model, column_name, column_name1, column_name2) %}

with validation as (

    select
        {{ column_name }} as column_name_sum
        , {{ column_name1 }} as column_name1
        , {{ column_name2 }} as column_name2

    from {{ model }}

),

validation_errors as (

    select
        *
    from validation
    where column_name1 + column_name2 != column_name_sum
)

select *
from validation_errors

{% endtest %}