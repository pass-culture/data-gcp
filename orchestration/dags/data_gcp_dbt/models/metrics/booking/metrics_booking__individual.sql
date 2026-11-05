select
    partition_month,
    user_is_beneficiary,
    region_name,
    department_name,
    epci_code,
    municipality_code,
    venue_region_name,
    venue_department_name,
    venue_epci_code,
    venue_municipality_code,
    offer_category_id,
    total_used_bookings,
    total_used_amount,
    total_created_bookings,
    total_created_free_bookings
from {{ ref("int_kpi__booking_individual") }}
