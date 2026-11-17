{#
    Active and cumulative activated cultural partners by venue type, month and
    venue geography (finest grain). Out of scope of the statistical secret (see
    exp_base__cultural_partner). A partner can match several types and tag_*
    rows are labels (subsets of a type): never sum over partner_type, use
    exp_base__cultural_partner for totals.
#}
{% set count_columns = [
    "total_active_partners_global",
    "total_active_partners_individual",
    "total_active_partners_collective",
    "total_active_partners_dual_part",
    "total_cumulative_activated_partners_global",
    "total_cumulative_activated_partners_individual",
    "total_cumulative_activated_partners_collective",
    "total_cumulative_activated_partners_dual_part",
    "total_cumulative_activated_partners_individual_only",
    "total_cumulative_activated_partners_collective_only",
] %}

select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_municipality_code,
    partner_type,
    {% for column in count_columns %}
        sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref("metrics_cultural_partner__activation_by_type") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_municipality_code,
    partner_type
