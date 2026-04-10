select
    deposit_expiration_month,
    is_statistic_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    epci_name,
    epci_code,
    city_name,
    city_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    offer_category_id,
    total_category_booked_beneficiaries
from {{ ref("metrics_diversity__by_category") }}
