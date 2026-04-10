select
    partition_month,
    is_statistic_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    epci_name,
    epci_code,
    city_name,
    city_code,
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_actual_beneficiaries,
    total_beneficiaries
from {{ ref("int_kpi__beneficiary") }}
