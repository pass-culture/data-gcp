select
    partition_month,
    venue_region_name,
    venue_region_code,
    venue_department_name,
    venue_department_code,
    venue_epci_name,
    venue_epci_code,
    venue_city_name,
    venue_city_code,
    total_created_collective_offers
from {{ ref("int_kpi__offer_collective") }}
