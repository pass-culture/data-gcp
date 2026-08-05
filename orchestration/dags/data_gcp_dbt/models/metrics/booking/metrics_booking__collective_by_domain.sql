/* Metrics-layer view of collective bookings by educational domain and institution geography. */
select
    partition_month,
    educational_domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    institution_epci_name,
    institution_epci_code,
    institution_city_code,
    total_collective_bookings,
    total_collective_booking_amount,
    total_collective_tickets,
    total_collective_institutions,
    cumulative_total_collective_bookings,
    cumulative_total_collective_booking_amount,
    cumulative_total_collective_tickets,
    cumulative_total_collective_institutions
from {{ ref("int_kpi__booking_collective_by_domain") }}
