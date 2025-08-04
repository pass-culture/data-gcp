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
        date_trunc(date(last_deposit_expiration_date), month) as execution_date,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_octroye_a_l_expiration_du_credit" as kpi_name,
        1 as denominator,
        avg(total_deposit_amount) as numerator,
        avg(total_deposit_amount) as kpi
    from {{ ref("mrt_global__user") }}
    {% if is_incremental() %}
        where
            date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date("{{ ds() }}"), month)
    {% endif %}
    group by execution_date, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
union all
{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(last_deposit_expiration_date), month) as execution_date,
        date("{{ ds() }}") as update_date,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_depense_a_l_expiration_du_credit" as kpi_name,
        1 as denominator,
        avg(total_actual_amount_spent) as numerator,
        avg(total_actual_amount_spent) as kpi
    from {{ ref("mrt_global__user") }}
    {% if is_incremental() %}
        where
            date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date("{{ ds() }}"), month)
    {% endif %}
    group by execution_date, update_date, dimension_name, dimension_value, kpi_name
{% endfor %}
