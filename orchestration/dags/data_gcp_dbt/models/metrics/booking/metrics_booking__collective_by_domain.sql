/* Metrics-layer view of collective bookings by educational domain and institution geography. */
select
    partition_month,
    domain_name,
    institution_region_name,
    institution_region_code,
    institution_department_name,
    institution_department_code,
    institution_epci_name,
    institution_epci_code,
    institution_city_code,
    total_collective_bookings_by_domain,
    total_collective_booking_amount_by_domain,
    total_collective_tickets_by_domain,
    total_collective_institutions_by_domain,
    cumulative_total_collective_bookings_by_domain,
    cumulative_total_collective_booking_amount_by_domain,
    cumulative_total_collective_tickets_by_domain,
    cumulative_total_collective_institutions_by_domain
from {{ ref("int_kpi__booking_collective_by_domain") }}
