select
    partition_month,
    scholar_year,
    venue_region_name,
    venue_region_code,
    venue_department_name,
    venue_department_code,
    venue_epci_name,
    venue_epci_code,
    venue_city_name,
    venue_city_code,
    total_collective_bookings,
    total_collective_amount_spent,
    cumulative_total_collective_bookings,
    cumulative_total_collective_amount_spent
from {{ ref("int_kpi__booking_collective") }}
