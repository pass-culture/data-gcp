{% test cardinality_equality(
    model,
    column_name,
    to,
    field,
    lookback_month=None,
    lookback_fields=["field_model", "field_to"]
) %}

    {%- set date_filter -%}
        {% if lookback_month is not none %}
            and {{ lookback_fields[0] }} >= date_sub(date("{{ ds() }}"), interval {{ lookback_month }} month)
        {% else %}

        {% endif %}
    {%- endset -%}
    {%- set date_filter_to -%}
        {% if lookback_month is not none %}
            and {{ lookback_fields[1] }} >= date_sub(date("{{ ds() }}"), interval {{ lookback_month }} month)
        {% else %}
        {% endif %}
    {%- endset -%}

    with
        filtered_model as (
            select {{ column_name }} from {{ model }} where true {{ date_filter }}
        ),

        filtered_to as (
            select {{ field }} from {{ to }} where true {{ date_filter_to }}
        ),

        table as (
            select
                (select count(distinct {{ field }}) from filtered_to) as num_rows_to,
                (
                    select count(distinct {{ column_name }}) from filtered_model
                ) as num_rows_table
        ),

        ratio_table as (select num_rows_table / num_rows_to as ratio from table)

    select 100 * (1 - ratio) as matching_percentage
    from ratio_table
    where ratio > {{ var("not_null_anomaly_threshold_alert_percentage") }}

{% endtest %}
