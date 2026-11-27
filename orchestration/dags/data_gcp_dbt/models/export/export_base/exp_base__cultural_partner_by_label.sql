{#
    Active and cumulative activated cultural partners by Ministry of Culture
    label, month and venue geography (finest grain). Out of scope of the
    statistical secret (see exp_base__cultural_partner). A partner can hold
    several labels: never sum over partner_label, use the 'ALL' rows (partners
    holding at least one label).
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
    partner_label,
    {% for column in count_columns %}
        sum({{ column }}) as {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref("metrics_cultural_partner__activation_by_label") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_municipality_code,
    partner_label
