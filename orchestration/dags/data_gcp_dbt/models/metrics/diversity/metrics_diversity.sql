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
    total_3plus_category_booked_beneficiaries,
    total_expired_credit_beneficiaries,
    cell_key_3plus,
    cell_key_expired
from {{ ref("int_kpi__diversity") }}
