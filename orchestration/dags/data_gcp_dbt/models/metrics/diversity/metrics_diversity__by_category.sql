select
    deposit_expiration_month,
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
    total_category_booked_beneficiaries,
    cell_key_category
from {{ ref("int_kpi__diversity_by_category") }}
