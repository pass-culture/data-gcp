{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("venue", "geo_full") %}
{% set categories = get_categories() %}

{% set kpis = [
    {"name": "total_reservations", "value_expr": "total_bookings"},
    {"name": "total_quantites", "value_expr": "total_quantities"},
    {"name": "total_ca", "value_expr": "total_revenue_amount"},
    {
        "name": "total_montant_rembourse",
        "value_expr": "total_reimbursed_amount",
    },
    {
        "name": "total_montant_contribution",
        "value_expr": "total_contribution_amount",
    },
] %}

with
    base_data as (
        select
            date_trunc(date(booking_used_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            venue_region_name,
            venue_department_name,
            venue_epci,
            venue_city,
            offer_category_id,
            offerer_is_epn,
            {% for kpi in kpis %}
                {{ kpi.value_expr }}{% if not loop.last %},{% endif %}
            {% endfor %}
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
                {% for kpi in kpis %}
                    {{ kpi.value_expr }}{% if not loop.last %},{% endif %}
                {% endfor %}
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
                coalesce(sum({{ kpi.value_expr }}), 0) as numerator,
                cast(1 as numeric) as denominator,
                coalesce(safe_divide(sum({{ kpi.value_expr }}), 1), 0) as kpi
            from dimension_cross
            group by partition_month, updated_at, dimension_name, dimension_value
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
        union all
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "pct_montant_contribution" as kpi_name,
            coalesce(sum(total_contribution_amount), 0) as numerator,
            coalesce(sum(total_revenue_amount), 0) as denominator,
            coalesce(
                safe_divide(sum(total_contribution_amount), sum(total_revenue_amount)),
                0
            ) as kpi
        from dimension_cross
        group by partition_month, updated_at, dimension_name, dimension_value
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
                    coalesce(sum({{ kpi.value_expr }}), 0) as numerator,
                    cast(1 as numeric) as denominator,
                    safe_divide(coalesce(sum({{ kpi.value_expr }}), 0), 1) as kpi
                from dimension_cross
                where offer_category_id = '{{ category.name }}'
                group by partition_month, updated_at, dimension_name, dimension_value
                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor %}
            union all
            select
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                'pct_montant_contribution_{{ category.value_expr }}' as kpi_name,
                coalesce(sum(total_contribution_amount), 0) as numerator,
                coalesce(sum(total_revenue_amount), 0) as denominator,
                safe_divide(
                    coalesce(sum(total_contribution_amount), 0),
                    coalesce(sum(total_revenue_amount), 0)
                ) as kpi
            from dimension_cross
            where offer_category_id = '{{ category.name }}'
            group by partition_month, updated_at, dimension_name, dimension_value
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
                coalesce(sum({{ kpi.value_expr }}), 0) as numerator,
                cast(1 as numeric) as denominator,
                safe_divide(coalesce(sum({{ kpi.value_expr }}), 0), 1) as kpi
            from dimension_cross
            where offerer_is_epn = true
            group by partition_month, updated_at, dimension_name, dimension_value
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
        union all
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "pct_montant_contribution_epn" as kpi_name,
            coalesce(sum(total_contribution_amount), 0) as numerator,
            coalesce(sum(total_revenue_amount), 0) as denominator,
            safe_divide(
                coalesce(sum(total_contribution_amount), 0),
                coalesce(sum(total_revenue_amount), 0)
            ) as kpi
        from dimension_cross
        where offerer_is_epn = true
        group by partition_month, updated_at, dimension_name, dimension_value
    )

select *
from global_metrics
union all
select *
from category_metrics
union all
select *
from epn_metrics
