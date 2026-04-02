with
    population_coverage as (
        select
            pop.population_snapshot_month,
            pop.population_birth_month,
            cast(pop.population_decimal_age as string) as population_decimal_age,
            pop.population_department_code,
            pop.population_department_name,
            pop.population_region_name,
            rd.region_code as population_region_code,
            pop.population_academy_name,
            pop.total_population
        from {{ ref("int_seed__monthly_france_population") }} as pop
        left join
            {{ ref("region_department") }} as rd
            on pop.population_department_code = rd.num_dep
    )

select
    population_snapshot_month as partition_month,
    population_birth_month as birth_month,
    population_decimal_age as milestone_age,
    population_department_code as department_code,
    population_department_name as department_name,
    population_region_name as region_name,
    population_region_code as region_code,
    sum(total_population) over (
        partition by population_decimal_age, population_department_code
        order by population_snapshot_month
        rows between 11 preceding and current row
    ) as total_population_last_12_months
from population_coverage
where population_decimal_age in ("15", "16", "17", "18", "19", "20")
