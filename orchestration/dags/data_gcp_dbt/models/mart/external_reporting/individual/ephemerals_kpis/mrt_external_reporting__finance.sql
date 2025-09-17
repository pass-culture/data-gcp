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
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

{% set categories = [
    {"name": "LIVRE", "value_expr": "livre"},
    {"name": "CINEMA", "value_expr": "cinema"},
    {"name": "MUSIQUE_LIVE", "value_expr": "concert"},
    {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
    {"name": "MUSEE", "value_expr": "musee"},
    {"name": "PRATIQUE_ART", "value_expr": "pratique_artistique"},
    {"name": "INSTRUMENT", "value_expr": "instrument"},
] %}

{% set kpis = [
    {
        "name": "total_reservations",
        "numerator_expr": "total_bookings",
        "denominator_expr": "1",
    },
    {
        "name": "total_quantites",
        "numerator_expr": "total_quantities",
        "denominator_expr": "1",
    },
    {
        "name": "total_ca",
        "numerator_expr": "total_revenue_amount",
        "denominator_expr": "1",
    },
    {
        "name": "total_montant_rembourse",
        "numerator_expr": "total_reimbursed_amount",
        "denominator_expr": "1",
    },
    {
        "name": "total_montant_contribution",
        "numerator_expr": "total_contribution_amount",
        "denominator_expr": "1",
    },
    {
        "name": "pct_montant_contribution",
        "numerator_expr": "total_contribution_amount",
        "denominator_expr": "total_revenue_amount",
    },
] %}

with
    base_data as (
        select
            date_trunc(date(booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            venue_region_name,
            venue_department_name,
            offer_category_id,
            offerer_is_epn,
            total_bookings,
            total_quantities,
            total_revenue_amount,
            total_reimbursed_amount,
            total_contribution_amount
        from {{ ref("mrt_finance__reimbursement") }}
        where
            1 = 1
            {% if is_incremental() %}
                and date_trunc(booking_used_date, month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ),

    dimension_cross as (
        {% for dim in dimensions %}
            select
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                partition_month,
                updated_at,
                offer_category_id,
                offerer_is_epn,
                total_bookings,
                total_quantities,
                total_revenue_amount,
                total_reimbursed_amount,
                total_contribution_amount
            from base_data
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    -- KPIs globaux (toutes catégories)
    global_metrics as (
        {% for kpi in kpis %}
            select
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                '{{ kpi.name }}' as kpi_name,
                sum({{ kpi.numerator_expr }}) as numerator,
                sum(cast({{ kpi.denominator_expr }} as int64)) as denominator,
                safe_divide(
                    sum({{ kpi.numerator_expr }}), sum({{ kpi.denominator_expr }})
                ) as kpi
            from dimension_cross
            group by partition_month, updated_at, dimension_name, dimension_value
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    -- KPIs par catégorie
    category_metrics as (
        {% for category in categories %}
            {% for kpi in kpis %}
                select
                    partition_month,
                    updated_at,
                    dimension_name,
                    dimension_value,
                    '{{ kpi.name }}_{{ category.value_expr }}' as kpi_name,
                    sum({{ kpi.numerator_expr }}) as numerator,
                    sum(cast({{ kpi.denominator_expr }} as int64)) as denominator,
                    safe_divide(
                        sum({{ kpi.numerator_expr }}),
                        sum(cast({{ kpi.denominator_expr }} as int64))
                    ) as kpi
                from dimension_cross
                where offer_category_id = '{{ category.name }}'
                group by partition_month, updated_at, dimension_name, dimension_value
                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor %}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    epn_metrics as (
        {% for kpi in kpis %}
            select
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                '{{ kpi.name }}_epn' as kpi_name,
                sum({{ kpi.numerator_expr }}) as numerator,
                sum(cast({{ kpi.denominator_expr }} as int64)) as denominator,
                safe_divide(
                    sum({{ kpi.numerator_expr }}),
                    sum(cast({{ kpi.denominator_expr }} as int64))
                ) as kpi
            from dimension_cross
            where offerer_is_epn = true
            group by partition_month, updated_at, dimension_name, dimension_value
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

select *
from global_metrics
union all
select *
from category_metrics
union all
select *
from epn_metrics
