select
    coalesce(m.partition_month, s.partition_month) as partition_month,
    coalesce(m.partner_region_name, s.partner_region_name) as partner_region_name,
    coalesce(m.partner_region_code, s.partner_region_code) as partner_region_code,
    coalesce(
        m.partner_department_name, s.partner_department_name
    ) as partner_department_name,
    coalesce(
        m.partner_department_code, s.partner_department_code
    ) as partner_department_code,
    coalesce(m.partner_epci_code, s.partner_epci_code) as partner_epci_code,
    coalesce(m.partner_city_code, s.partner_city_code) as partner_city_code,
    coalesce(m.partner_type, s.partner_type) as partner_type,
    m.total_active_partners_individual,
    m.total_active_partners_collective,
    m.total_active_partners_global,
    m.total_active_partners_dual_part,
    m.total_cumulative_activated_partners_individual,
    m.total_cumulative_activated_partners_collective,
    m.total_cumulative_activated_partners_global,
    m.total_cumulative_activated_partners_individual_only,
    m.total_cumulative_activated_partners_collective_only,
    m.total_cumulative_activated_partners_dual_part,
    s.total_offerers_created_by_cohort,
    s.total_activated_offerer_consultation_or_adage_30d_by_cohort,
    s.total_activated_offerer_offer_or_adage_30d_by_cohort,
    s.total_activated_offerer_global_30d_by_cohort,
    s.total_activated_offerer_individual_30d_by_cohort,
    s.total_activated_offerer_collective_30d_by_cohort
from {{ ref("int_kpi__cultural_partner_activation_by_type") }} as m
full outer join
    {{ ref("int_kpi__cultural_partner_activation_cohorts_by_type") }} as s
    on m.partition_month = s.partition_month
    and m.partner_city_code = s.partner_city_code
    and m.partner_epci_code = s.partner_epci_code
    and m.partner_department_code = s.partner_department_code
    and m.partner_region_code = s.partner_region_code
    and m.partner_type = s.partner_type
