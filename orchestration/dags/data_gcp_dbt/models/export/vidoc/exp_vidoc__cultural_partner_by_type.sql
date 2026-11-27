-- A partner can match several types and tag_* rows are labels (subsets of a
-- type): never sum this table over partner_type, use
-- exp_vidoc__cultural_partner for totals.
select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_type,
    sum(total_active_partners_individual) as total_active_partners_individual,
    sum(total_active_partners_collective) as total_active_partners_collective,
    sum(total_active_partners_global) as total_active_partners_global,
    sum(total_active_partners_dual_part) as total_active_partners_dual_part
from {{ ref("exp_base__cultural_partner_by_type") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_type
