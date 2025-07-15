{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "user_region_name"},
    {"name": "DEP", "value_expr": "user_department_name"},
] %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(user_expiration_month), month) as execution_date,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "pct_beneficiaires_ayant_reserve_dans_3_categories" as kpi_name,
        sum(total_users) as denominator,
        sum(total_3_category_booked_users) as numerator,
        safe_divide(sum(total_3_category_booked_users), sum(total_users)) as kpi
    from {{ ref("mrt_native__outgoing_cohort") }}
    {% if is_incremental() %}
        where
            date_trunc(user_expiration_month, month)
            = date_trunc(date("{{ ds() }}"), month)
    {% endif %}
    group by execution_date, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
