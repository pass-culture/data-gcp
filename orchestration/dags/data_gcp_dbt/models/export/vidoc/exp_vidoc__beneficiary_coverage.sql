with source as (select * from {{ ref("metrics_beneficiary__coverage") }})

select
    s.partition_month,
    s.region_name,
    s.region_code,
    s.department_name,
    s.department_code,
    s.milestone_age,
    greatest(
        s.total_beneficiaries_last_12_months + pt.perturbation, 0
    ) as total_beneficiaries_last_12_months
from source as s
inner join
    {{ ref("perturbation_table__individual") }} as pt
    on s.total_beneficiaries_last_12_months between pt.count_min and pt.count_max
    and s.cell_key_beneficiaries between pt.cell_key_min and pt.cell_key_max
