{#
    Active and cumulative activated cultural partners (venues) by month and venue
    geography, at the finest grain (municipality). Partner lists are public:
    counts are not perturbed and not under statistical secret. Venue counts are
    additive across geographic cells (a venue has one location), so every tool
    sums these cells. EPN entities are not counted yet, as in metrics.
#}
{% set count_columns = [
    "total_active_partners_global",
    "total_active_partners_individual",
    "total_active_partners_collective",
    "total_active_partners_dual_part",
    "total_active_partners_individual_only",
    "total_active_partners_collective_only",
    "total_cumulative_activated_partners_global",
    "total_cumulative_activated_partners_individual",
    "total_cumulative_activated_partners_collective",
    "total_cumulative_activated_partners_dual_part",
    "total_cumulative_activated_partners_individual_only",
    "total_cumulative_activated_partners_collective_only",
    "total_partners_with_collective_offer",
] %}

select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_academy_name,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_municipality_code,
    {% for column in count_columns %}
        sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref("metrics_cultural_partner__activation") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_academy_name,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_municipality_code
