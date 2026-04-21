select
    partition_month,
    region_name,
    region_code,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries_last_12_months,
    cell_key_beneficiaries
from {{ ref("int_kpi__beneficiary_coverage") }}
