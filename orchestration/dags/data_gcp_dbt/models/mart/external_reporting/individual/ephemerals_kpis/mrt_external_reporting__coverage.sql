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
    {"name": "REG", "value_expr": "population_region_name"},
    {"name": "DEP", "value_expr": "population_department_name"},
] %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(population_snapshot_month, month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_couverture_18" as kpi_name,
        sum(total_users_last_12_months) as numerator,
        sum(total_population_last_12_months) as denominator,
        safe_divide(
            sum(total_users_last_12_months), sum(total_population_last_12_months)
        ) as kpi
    from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
    where
        population_decimal_age = "19"
        and population_snapshot_month <= date_trunc(current_date, month)
        {% if is_incremental() %}
            and date_trunc(population_snapshot_month, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    select
        date_trunc(population_snapshot_month, month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_couverture_17" as kpi_name,
        sum(total_users_last_12_months) as numerator,
        sum(total_population_last_12_months) as denominator,
        safe_divide(
            sum(total_users_last_12_months), sum(total_population_last_12_months)
        ) as kpi
    from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
    where
        population_decimal_age = "18"
        and population_snapshot_month <= date_trunc(current_date, month)
        {% if is_incremental() %}
            and date_trunc(population_snapshot_month, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    select
        date_trunc(population_snapshot_month, month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_couverture_16" as kpi_name,
        sum(total_users_last_12_months) as numerator,
        sum(total_population_last_12_months) as denominator,
        safe_divide(
            sum(total_users_last_12_months), sum(total_population_last_12_months)
        ) as kpi
    from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
    where
        population_decimal_age = "17"
        and population_snapshot_month <= date_trunc(current_date, month)
        {% if is_incremental() %}
            and date_trunc(population_snapshot_month, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    union all
    select
        date_trunc(population_snapshot_month, month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "taux_couverture_15" as kpi_name,
        sum(total_users_last_12_months) as numerator,
        sum(total_population_last_12_months) as denominator,
        safe_divide(
            sum(total_users_last_12_months), sum(total_population_last_12_months)
        ) as kpi
    from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
    where
        population_decimal_age = "16"
        and population_snapshot_month <= date_trunc(current_date, month)
        {% if is_incremental() %}
            and date_trunc(population_snapshot_month, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
