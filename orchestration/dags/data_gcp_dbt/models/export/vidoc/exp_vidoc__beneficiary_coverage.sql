-- vidoc export of the perturbed base, see exp_base__beneficiary_coverage
select
    partition_month,
    region_name,
    region_code,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries_last_12_months
from {{ ref("exp_base__beneficiary_coverage") }}
