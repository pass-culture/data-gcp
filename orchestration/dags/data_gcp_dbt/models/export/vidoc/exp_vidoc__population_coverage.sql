select
    partition_month,
    birth_month,
    milestone_age,
    department_code,
    department_name,
    region_name,
    region_code,
    total_population_last_12_months
from {{ ref("metrics_population__coverage") }}
