{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
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
        date_trunc(date(last_deposit_expiration_date), month) as partition_month,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_octroye_a_l_expiration_du_credit" as kpi_name,
        count(distinct user_id) as denominator,
        sum(total_deposit_amount) as numerator,
        safe_divide(sum(total_deposit_amount), count(distinct user_id)) as kpi
    from {{ ref("mrt_global__user") }}
    {% if is_incremental() %}
        where
            date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
union all
{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(last_deposit_expiration_date), month) as partition_month,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_depense_a_l_expiration_du_credit" as kpi_name,
        count(distinct user_id) as denominator,
        sum(total_actual_amount_spent) as numerator,
        safe_divide(sum(total_actual_amount_spent), count(distinct user_id)) as kpi
    from {{ ref("mrt_global__user") }}
    {% if is_incremental() %}
        where
            date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
