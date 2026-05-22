select
    partition_month,
    region_name,
    department_name,
    department_code,
    epci_code,
    epci_name,
    city_code,
    city_name,
    is_in_qpv,
    micro_density_label,
    macro_density_label,
    total_expired_beneficiaries,
    total_deposit_amount_at_expiration,
    average_deposit_amount_at_expiration,
    total_actual_amount_spent_at_expiration,
    average_actual_amount_spent_at_expiration
from {{ ref("int_kpi__beneficiary_expired_cohorts") }}
