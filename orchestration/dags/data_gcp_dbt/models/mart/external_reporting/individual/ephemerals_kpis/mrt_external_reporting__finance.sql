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
                sum({{ kpi.value_expr }}) as numerator,
                cast(1 as numeric) as denominator,
                safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
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
            sum(total_contribution_amount) as numerator,
            sum(total_revenue_amount) as denominator,
            safe_divide(
                sum(total_contribution_amount), sum(total_revenue_amount)
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
                    sum({{ kpi.value_expr }}) as numerator,
                    cast(1 as numeric) as denominator,
                    safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
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
                sum(total_contribution_amount) as numerator,
                sum(total_revenue_amount) as denominator,
                safe_divide(
                    sum(total_contribution_amount), sum(total_revenue_amount)
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
                sum({{ kpi.value_expr }}) as numerator,
                cast(1 as numeric) as denominator,
                safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
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
            sum(total_contribution_amount) as numerator,
            sum(total_revenue_amount) as denominator,
            safe_divide(
                sum(total_contribution_amount), sum(total_revenue_amount)
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
