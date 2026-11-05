-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{#
    Individual bookings used in the month (venue geography), rolled up from
    metrics_booking__finance_individual: all categories, by category (kpi
    suffixed with the category) and EPN offerers only (suffix _epn).
#}
{% set categories = get_categories() %}
{% set kpis = {
    "total_reservations": "total_bookings",
    "total_quantites": "total_quantities",
    "total_ca": "total_revenue_amount",
    "total_montant_rembourse": "total_reimbursed_amount",
    "total_montant_contribution": "total_contribution_amount",
} %}

with
    dimension_cross as (
        {% for dim in get_dimensions("venue", "geo_full") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                date(partition_month) as partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                offer_category_id,
                offerer_is_epn,
                {% for column in kpis.values() %}
                    sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__finance_individual") }}
            where
                1 = 1
                {% if is_incremental() %}
                    and date(partition_month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month,
                dimension_name,
                dimension_value,
                offer_category_id,
                offerer_is_epn
        {% endfor %}
    ),

    -- kpi_suffix: '' for all categories, '_<category>' or '_epn'
    scoped_metrics as (
        select
            partition_month,
            dimension_name,
            dimension_value,
            '' as kpi_suffix,
            {% for column in kpis.values() %}
                sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from dimension_cross
        group by partition_month, dimension_name, dimension_value
        union all
        select
            partition_month,
            dimension_name,
            dimension_value,
            case
                offer_category_id
                {% for category in categories %}
                    when '{{ category.name }}' then '_{{ category.value_expr }}'
                {% endfor %}
            end as kpi_suffix,
            {% for column in kpis.values() %}
                sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from dimension_cross
        where
            offer_category_id in (
                {% for category in categories %}
                    '{{ category.name }}'{% if not loop.last %},{% endif %}
                {% endfor %}
            )
        group by partition_month, dimension_name, dimension_value, kpi_suffix
        union all
        select
            partition_month,
            dimension_name,
            dimension_value,
            '_epn' as kpi_suffix,
            {% for column in kpis.values() %}
                sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from dimension_cross
        where offerer_is_epn and dimension_value is not null
        group by partition_month, dimension_name, dimension_value
    )

select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    concat(kpi_name, kpi_suffix) as kpi_name,
    numerator,
    cast(1 as numeric) as denominator,
    numerator as kpi
from
    (
        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_suffix,
            {% for kpi_name, column in kpis.items() %}
                cast(coalesce({{ column }}, 0) as float64) as {{ kpi_name }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from scoped_metrics
    )
    unpivot (numerator for kpi_name in ({{ kpis.keys() | join(", ") }}))
union all
select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    concat("pct_montant_contribution", kpi_suffix) as kpi_name,
    cast(coalesce(total_contribution_amount, 0) as float64) as numerator,
    coalesce(total_revenue_amount, 0) as denominator,
    -- only the all categories share falls back to 0 when there is no revenue
    if(
        kpi_suffix = '',
        coalesce(safe_divide(total_contribution_amount, total_revenue_amount), 0),
        safe_divide(
            coalesce(total_contribution_amount, 0), coalesce(total_revenue_amount, 0)
        )
    ) as kpi
from scoped_metrics
