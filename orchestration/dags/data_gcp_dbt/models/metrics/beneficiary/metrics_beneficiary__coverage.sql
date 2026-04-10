select
    partition_month,
    is_statistic_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries_last_12_months
from {{ ref("int_kpi__beneficiary_coverage") }}
