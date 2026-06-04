select
    partition_month,
    venue_department_code,
    venue_department_name,
    venue_region_name,
    venue_epci_code,
    venue_city_code,
    offerer_is_epn,
    offer_category_id,
    total_bookings,
    total_quantities,
    total_revenue_amount,
    total_reimbursed_amount,
    total_contribution_amount
from {{ ref("int_kpi__booking_finance_individual") }}
