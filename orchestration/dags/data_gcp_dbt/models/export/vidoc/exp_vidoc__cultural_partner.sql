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
    sum(total_active_partners_dual_part) as total_active_partners_dual_part,
    sum(
        total_cumulative_activated_partners_individual
    ) as total_cumulative_activated_partners_individual,
    sum(
        total_cumulative_activated_partners_collective
    ) as total_cumulative_activated_partners_collective,
    sum(
        total_cumulative_activated_partners_global
    ) as total_cumulative_activated_partners_global,
    sum(
        total_cumulative_activated_partners_individual_only
    ) as total_cumulative_activated_partners_individual_only,
    sum(
        total_cumulative_activated_partners_collective_only
    ) as total_cumulative_activated_partners_collective_only,
    sum(
        total_cumulative_activated_partners_dual_part
    ) as total_cumulative_activated_partners_dual_part
from {{ ref("metrics_cultural_partner__activation_by_type") }}
group by
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_type
