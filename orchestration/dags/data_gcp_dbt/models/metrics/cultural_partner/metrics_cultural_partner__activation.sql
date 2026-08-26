select
    m.partition_month,
    m.partner_region_name,
    m.partner_region_code,
    m.partner_department_name,
    m.partner_department_code,
    m.partner_epci_code,
    m.partner_city_code,
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
    coalesce(s.total_offerers_created_by_cohort, 0) as total_offerers_created_by_cohort,
    coalesce(
        s.total_activated_offerer_consultation_or_adage_30d_by_cohort, 0
    ) as total_activated_offerer_consultation_or_adage_30d_by_cohort,
    coalesce(
        s.total_activated_offerer_offer_or_adage_30d_by_cohort, 0
    ) as total_activated_offerer_offer_or_adage_30d_by_cohort,
    coalesce(
        s.total_activated_offerer_global_30d_by_cohort, 0
    ) as total_activated_offerer_global_30d_by_cohort,
    coalesce(
        s.total_activated_offerer_individual_30d_by_cohort, 0
    ) as total_activated_offerer_individual_30d_by_cohort,
    coalesce(
        s.total_activated_offerer_collective_30d_by_cohort, 0
    ) as total_activated_offerer_collective_30d_by_cohort
from {{ ref("int_kpi__cultural_partner_activation") }} as m
left join
    {{ ref("int_kpi__cultural_partner_activation_cohorts") }} as s
    on m.partition_month = s.partition_month
    and m.partner_city_code = s.partner_city_code
    and m.partner_epci_code = s.partner_epci_code
    and m.partner_department_code = s.partner_department_code
    and m.partner_region_code = s.partner_region_code
