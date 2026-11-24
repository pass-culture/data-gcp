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
    territory stays visible while small territories show no breakdown. The
    reports also show the breakdown over the last 6 months up to the selected
    month, so the territory needs that many distinct bookers in every 6-month
    window ending in the last 12 months too (a territory missing from a window
    has none).

    Distinct bookers do not add up across territories, but new bookers do (a
    beneficiary has one geography and is new once): the distinct bookers of a
    territory since 2022 are the sum of its new bookers since 2022, all
    categories (total_new_bookers) or in one category
    (total_new_category_bookers).
#}
{% set min_bookers = 5 %}
{% set min_category_breakdown_bookers = 20 %}
{% set breakdown_window_months = 6 %}
{% set breakdown_window_ends = 12 %}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
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

with
    -- distinct bookers by month of use, with the beneficiary geography
    booker_months as (
        select distinct
            b.user_id,
            date_trunc(date(b.booking_used_date), month) as booking_month,
            u.user_region_name as region_name,
            u.user_department_name as department_name,
            u.user_epci_code as epci_code,
            u.user_municipality_code as municipality_code
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
        where
            b.booking_is_used
            and date_trunc(date(b.booking_used_date), month) between date_sub(
                {{ last_month }},
                interval {{ breakdown_window_ends + breakdown_window_months - 2 }} month
            ) and {{ last_month }}
    ),

    window_bookers as (
        select window_end, booker_months.*
        from
            unnest(
                generate_date_array(
                    date_sub(
                        {{ last_month }}, interval {{ breakdown_window_ends - 1 }} month
                    ),
                    {{ last_month }},
                    interval 1 month
                )
            ) as window_end
        inner join
            booker_months
            on booker_months.booking_month between date_sub(
                window_end, interval {{ breakdown_window_months - 1 }} month
            ) and window_end
    ),

    -- fewest distinct bookers of the territory over its 6-month windows
    min_window_bookers as (
        select
            dimension_name,
            dimension_value,
            if(
                count(*) < {{ breakdown_window_ends }}, 0, min(window_total_bookers)
            ) as min_window_total_bookers
        from
            (
                {% for dim in get_dimensions("beneficiary", "geo_full") %}
                    {% if not loop.first %}
                        union all
                    {% endif %}
                    select
                        '{{ dim.name }}' as dimension_name,
                        {{ dim.value_expr }} as dimension_value,
                        window_end,
                        count(distinct user_id) as window_total_bookers
                    from window_bookers
                    group by dimension_name, dimension_value, window_end
                {% endfor %}
            )
        where dimension_value is not null
        group by dimension_name, dimension_value
    ),

    history_bookers as (
        select dimension_name, dimension_value, kpi_name, total_beneficiaries
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
                    where partition_month <= {{ last_month }}
                    group by dimension_name, dimension_value
                {% endfor %}
            ) unpivot (
                total_beneficiaries for kpi_name in ({{ kpis.keys() | join(", ") }})
            )
        where dimension_value is not null
    )

select
    h.dimension_name,
    h.dimension_value,
    h.kpi_name,
    -- the breakdown counts the fewest bookers among the history and its windows;
    -- a window without bookers counts as 1 so that the breakdown is masked
    if(
        starts_with(h.kpi_name, 'pct_reservations_beneficiaires_'),
        least(
            h.total_beneficiaries, greatest(coalesce(w.min_window_total_bookers, 0), 1)
        ),
        h.total_beneficiaries
    ) as total_beneficiaries,
    if(
        starts_with(h.kpi_name, 'pct_reservations_beneficiaires_'),
        {{ min_category_breakdown_bookers }},
        {{ min_bookers }}
    ) as min_beneficiaries
from history_bookers as h
left join
    min_window_bookers as w
    on h.dimension_name = w.dimension_name
    and h.dimension_value = w.dimension_value
