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
    Used bookings made by the beneficiaries living in the territory (user
    geography), wherever they booked, in total and by offer category (suffix,
    see get_categories). Monthly flows by month of use: sum the months for
    "since the generalisation", the last 6 months for the last semester.
    Bookings of users missing from mrt_global__user_beneficiary (deleted,
    suspended, GRANT_FREE) are left out, NAT included.
#}
{% set kpis = {
    "total_reservations_beneficiaires": "sum(total_used_bookings)",
    "total_montant_beneficiaires": "sum(total_used_amount)",
} %}
{% for category in get_categories() %}
    {% do kpis.update(
        {
            "total_reservations_beneficiaires_"
            ~ category.value_expr: "sum(case when offer_category_id = '"
            ~ category.name
            ~ "' then total_used_bookings end)",
            "total_montant_beneficiaires_"
            ~ category.value_expr: "sum(case when offer_category_id = '"
            ~ category.name
            ~ "' then total_used_amount end)",
        }
    ) %}
{% endfor %}

with
    aggregated as (
        {% for dim in get_dimensions("beneficiary", "geo_full") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for kpi_name, expression in kpis.items() %}
                    cast(coalesce({{ expression }}, 0) as float64) as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__individual") }}
            where
                user_is_beneficiary and total_used_bookings > 0
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    )

select
    partition_month,
    timestamp('{{ ts() }}') as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    1 as denominator,
    numerator as kpi
from aggregated unpivot (numerator for kpi_name in ({{ kpis.keys() | join(", ") }}))
where dimension_value is not null
-- share of each category in the bookings of the territory: sum numerator and
-- denominator over the months, then divide
{% for category in get_categories() %}
    union all
    select
        partition_month,
        timestamp('{{ ts() }}') as updated_at,
        dimension_name,
        dimension_value,
        'pct_reservations_beneficiaires_{{ category.value_expr }}' as kpi_name,
        total_reservations_beneficiaires_{{ category.value_expr }} as numerator,
        total_reservations_beneficiaires as denominator,
        safe_divide(
            total_reservations_beneficiaires_{{ category.value_expr }},
            total_reservations_beneficiaires
        ) as kpi
    from aggregated
    where dimension_value is not null
{% endfor %}
