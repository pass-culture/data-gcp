-- A partner can hold several labels: never sum this table over partner_label,
-- the 'ALL' rows count the partners holding at least one label.
select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_label,
    sum(total_active_partners_individual) as total_active_partners_individual,
    sum(total_active_partners_collective) as total_active_partners_collective,
    sum(total_active_partners_global) as total_active_partners_global,
    sum(total_active_partners_dual_part) as total_active_partners_dual_part
from {{ ref("exp_base__cultural_partner_by_label") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_label
