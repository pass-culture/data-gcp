select
    partition_month,
    region_name,
    department_name,
    epci_code,
    municipality_code,
    offer_category_id,
    total_new_bookers,
    total_new_category_bookers
from {{ ref("int_kpi__booking_individual_bookers") }}
