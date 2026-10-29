{{ config(materialized="table") }}

{#
    Distinct beneficiaries behind the beneficiary booking KPIs of a territory
    (user geography), over the whole reporting history (2022 to the last complete
    month). Used by external_reporting__individual to mask the KPIs that fewer
    than 5 beneficiaries account for: amounts and booking counts cannot be
    blurred, and the monthly flows are summed by the reports.
#}
{% set kpis = {
    "total_reservations_beneficiaires": "true",
    "total_montant_beneficiaires": "true",
    "part_partenaires_dans_conso_beneficiaires_territoire_montant": "true",
    "part_partenaires_dans_conso_beneficiaires_territoire_volume": "true",
} %}
{% for category in get_categories() %}
    {% do kpis.update(
        {
            "total_reservations_beneficiaires_"
            ~ category.value_expr: "b.offer_category_id = '"
            ~ category.name
            ~ "'",
            "total_montant_beneficiaires_"
            ~ category.value_expr: "b.offer_category_id = '"
            ~ category.name
            ~ "'",
            "pct_reservations_beneficiaires_"
            ~ category.value_expr: "b.offer_category_id = '"
            ~ category.name
            ~ "'",
        }
    ) %}
{% endfor %}

with
    base_booking_data as (
        select
            b.user_id,
            b.offer_category_id,
            u.user_region_name,
            u.user_department_name,
            u.user_epci_code,
            u.user_municipality_code
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
        where
            b.booking_is_used
            and date(b.booking_used_date) >= date('2022-01-01')
            and date_trunc(date(b.booking_used_date), month)
            <= date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
    )

select dimension_name, dimension_value, kpi_name, total_beneficiaries
from
    (
        {% for dim in get_dimensions("user", "geo_full") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for kpi_name, condition in kpis.items() %}
                    count(
                        distinct case when {{ condition }} then b.user_id end
                    ) as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from base_booking_data as b
            group by dimension_name, dimension_value
        {% endfor %}
    )
    unpivot (total_beneficiaries for kpi_name in ({{ kpis.keys() | join(", ") }}))
where dimension_value is not null
