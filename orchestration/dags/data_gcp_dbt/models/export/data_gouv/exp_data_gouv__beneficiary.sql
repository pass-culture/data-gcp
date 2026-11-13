-- data.gouv.fr beneficiaries, from the perturbed base: same perturbed counts,
-- closed months only, geographic codes formatted for data.gouv.fr
with
    cells as (
        select
            partition_month,
            {{ data_gouv_geo_columns() }},
            -- ages that no credit history allows (inconsistent declared birth
            -- dates) are published as unknown, like missing birth dates. The age
            -- is measured on the first day of the month, so a young person
            -- credited at 15 during the month is 14. Youngest: 14 until the March
            -- 2025 reform (no credit at 15 since), then one more year per year.
            -- Oldest: 19 in February 2019 (credit at 18, open until the 19th
            -- birthday), then one more year per year
            if(
                age_at_calculation between if(
                    partition_month < "2025-03-01",
                    14,
                    14 + div(
                        date_diff(partition_month, date("2025-02-01"), month) + 11, 12
                    )
                ) and 19
                + div(date_diff(partition_month, date("2019-02-01"), month), 12),
                age_at_calculation,
                null
            ) as age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            total_beneficiaries,
            total_actual_beneficiaries
        from {{ ref("exp_base__beneficiary") }}
        -- from May 2021: national opening of the individual credit at 18 (pilot
        -- before)
        where {{ data_gouv_month_filter("partition_month", "2021-05-01") }}
    )

-- sums of perturbed cells: still 0 or >= 5, and equal to the other exports' sums
select
    partition_month,
    region_code,
    region_name,
    department_code,
    department_name,
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    sum(total_beneficiaries) as total_beneficiaries,
    sum(total_actual_beneficiaries) as total_actual_beneficiaries
from cells
group by
    partition_month,
    region_code,
    region_name,
    department_code,
    department_name,
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label
-- a row whose counts are all 0 is a cell of 1 to 5 young people perturbed to 0:
-- left out, so that a missing combination reads as 0 or a very small count
having sum(cells.total_beneficiaries) > 0
