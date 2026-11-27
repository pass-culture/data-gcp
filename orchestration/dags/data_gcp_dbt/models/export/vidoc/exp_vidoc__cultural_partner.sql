-- vidoc export of exp_base__cultural_partner (counts of establishments, out of
-- scope of the statistical secret), summed to the department.
select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    sum(total_active_partners_global) as total_active_partners_global,
    sum(total_active_partners_individual) as total_active_partners_individual,
    sum(total_active_partners_collective) as total_active_partners_collective,
    sum(total_active_partners_dual_part) as total_active_partners_dual_part,
    sum(total_active_partners_individual_only) as total_active_partners_individual_only,
    sum(total_active_partners_collective_only) as total_active_partners_collective_only
from {{ ref("exp_base__cultural_partner") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code
