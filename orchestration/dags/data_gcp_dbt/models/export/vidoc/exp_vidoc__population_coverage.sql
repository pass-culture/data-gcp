select
    partition_month,
    birth_month,
    cast(milestone_age as int64) as milestone_age,
    department_code,
    department_name,
    region_name,
    region_code,
    total_population_last_12_months
from {{ ref("metrics_population__coverage") }}
where milestone_age in ("16", "17", "18", "19")
