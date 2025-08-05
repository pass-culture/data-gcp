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
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

{% set categories = [
    {"name": "LIVRE", "value_expr": "livres"},
    {"name": "CINEMA", "value_expr": "cinema"}, 
    {"name": "MUSIQUE_LIVE", "value_expr": "musique_live"},
    {"name": "SPECTACLE", "value_expr": "spectacle_vivant"},
    {"name": "MUSEE", "value_expr": "musee"},
    {"name": "PRATIQUE_ART", "value_expr": "pratiques_artistiques"},
] %}

{% set kpis = [
    {"name": "total_reservations", "value_expr": "total_bookings"},
    {"name": "total_quantites", "value_expr": "total_quantities"},
    {"name": "total_ca", "value_expr": "total_revenue_amount"},
    {"name": "total_montant_rembourse", "value_expr": "total_reimbursed_amount"},
    {"name": "total_montant_contribution", "value_expr": "total_contribution_amount"},
] %}

with base_data as (
    select
        date_trunc(date(booking_used_date), month) as execution_date,
        date("{{ ds() }}") as update_date,
        venue_region_name,
        venue_department_name,
        offer_category_id,
        {% for kpi in kpis %}
        {{ kpi.value_expr }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref("mrt_finance__reimbursement") }}
    where
        1 = 1
        {% if is_incremental() %}
            and date_trunc(booking_used_date, month) = date_trunc(date("{{ ds() }}"), month)
        {% endif %}
),

dimension_cross as (
    {% for dim in dimensions %}
    select
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        execution_date,
        update_date,
        offer_category_id,
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
        execution_date,
        update_date,
        dimension_name,
        dimension_value,
        '{{ kpi.name }}' as kpi_name,
        sum({{ kpi.value_expr }}) as numerator,
        1 as denominator,
        safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
    from dimension_cross
    group by execution_date, update_date, dimension_name, dimension_value
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
        execution_date,
        update_date,
        dimension_name,
        dimension_value,
        '{{ kpi.name }}_{{ category.value_expr }}' as kpi_name,
        sum({{ kpi.value_expr }}) as numerator,
        1 as denominator,
        safe_divide(sum({{ kpi.value_expr }}), 1) as kpi
    from dimension_cross
    where offer_category_id = '{{ category.name }}'
    group by execution_date, update_date, dimension_name, dimension_value
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select * from global_metrics
union all
select * from category_metrics