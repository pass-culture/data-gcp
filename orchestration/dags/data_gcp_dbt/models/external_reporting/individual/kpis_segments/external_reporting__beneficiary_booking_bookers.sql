{{ config(materialized="table") }}

{#
    Distinct beneficiaries behind the beneficiary booking KPIs of a territory
    (user geography), over the whole reporting history (2022 to the last complete
    month). Used by external_reporting__individual to mask the KPIs that fewer
    than 5 beneficiaries account for: amounts and booking counts cannot be
    blurred, and the monthly flows are summed by the reports.

    The category shares (pct_reservations_beneficiaires_*) are published as a
    whole breakdown: they count every booker of the territory and need at least
    min_category_breakdown_bookers of them, so that a small category of a large
    territory stays visible while small territories show no breakdown.

    Distinct bookers do not add up across territories, but new bookers do (a
    beneficiary has one geography and is new once): the distinct bookers of a
    territory since 2022 are the sum of its new bookers since 2022, all
    categories (total_new_bookers) or in one category
    (total_new_category_bookers).
#}
{% set min_bookers = 5 %}
{% set min_category_breakdown_bookers = 20 %}
{% set kpis = {
    "total_reservations_beneficiaires": "sum(total_new_bookers)",
    "total_montant_beneficiaires": "sum(total_new_bookers)",
    "part_partenaires_dans_conso_beneficiaires_territoire_montant": "sum(total_new_bookers)",
    "part_partenaires_dans_conso_beneficiaires_territoire_volume": "sum(total_new_bookers)",
} %}
{% for category in get_categories() %}
    {% do kpis.update(
        {
            "total_reservations_beneficiaires_"
            ~ category.value_expr: "sum(if(offer_category_id = '"
            ~ category.name
            ~ "', total_new_category_bookers, 0))",
            "total_montant_beneficiaires_"
            ~ category.value_expr: "sum(if(offer_category_id = '"
            ~ category.name
            ~ "', total_new_category_bookers, 0))",
            "pct_reservations_beneficiaires_"
            ~ category.value_expr: "sum(total_new_bookers)",
        }
    ) %}
{% endfor %}

select
    dimension_name,
    dimension_value,
    kpi_name,
    total_beneficiaries,
    if(
        starts_with(kpi_name, 'pct_reservations_beneficiaires_'),
        {{ min_category_breakdown_bookers }},
        {{ min_bookers }}
    ) as min_beneficiaries
from
    (
        {% for dim in get_dimensions("beneficiary", "geo_full") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for kpi_name, expression in kpis.items() %}
                    {{ expression }} as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__individual_bookers") }}
            where
                partition_month
                <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
            group by dimension_name, dimension_value
        {% endfor %}
    )
    unpivot (total_beneficiaries for kpi_name in ({{ kpis.keys() | join(", ") }}))
where dimension_value is not null
