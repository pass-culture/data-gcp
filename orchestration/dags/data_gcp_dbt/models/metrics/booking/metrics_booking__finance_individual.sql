select
    bf.partition_month,
    bf.venue_department_code,
    bf.venue_department_name,
    bf.venue_region_name,
    bf.venue_epci_code,
    bf.venue_city_code,
    bf.offerer_is_epn,
    bf.offer_category_id,
    bf.total_bookings,
    bf.total_quantities,
    bf.total_revenue_amount,
    bf.total_reimbursed_amount,
    bf.total_contribution_amount,
    coalesce(cp.total_active_partners_global <= 3, true) as is_statistic_secret
from {{ ref("int_kpi__booking_finance_individual") }} as bf
left join
    {{ ref("int_kpi__cultural_partner_activation") }} as cp
    on bf.venue_city_code = cp.partner_city_code
    and bf.partition_month = cp.partition_month
