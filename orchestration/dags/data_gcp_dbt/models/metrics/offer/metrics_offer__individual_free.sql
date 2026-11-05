select
    partition_month,
    venue_region_name,
    venue_department_name,
    venue_epci_code,
    venue_municipality_code,
    total_created_free_offers,
    total_created_free_bookable_offers,
    cumulative_total_created_free_offers,
    cumulative_total_created_free_bookable_offers
from {{ ref("int_kpi__offer_individual_free") }}
