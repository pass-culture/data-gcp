select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_academy_name,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_city_code,
    partner_municipality_code,
    total_new_partners_with_template_offer,
    total_cumulative_partners_with_template_offer
from {{ ref("int_kpi__cultural_partner_template_offer") }}
