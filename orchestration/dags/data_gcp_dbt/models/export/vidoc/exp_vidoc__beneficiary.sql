-- vidoc export of the perturbed base, see exp_base__beneficiary
select
    partition_month,
    region_name,
    region_code,
    department_name,
    department_code,
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_beneficiaries,
    total_actual_beneficiaries
from {{ ref("exp_base__beneficiary") }}
