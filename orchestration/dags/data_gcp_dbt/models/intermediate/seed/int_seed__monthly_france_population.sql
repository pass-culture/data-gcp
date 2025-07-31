{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
    )
}}

select
    pop.decimal_age as population_decimal_age,
    pop.department_code as population_department_code,
    pop.department_name as population_department_name,
    dep.academy_name as population_academy_name,
    dep.region_name as population_region_name,
    date(pop.current_date) as population_snapshot_month,
    date(pop.born_date) as population_birth_month,
    sum(pop.population) as total_population
from {{ source("seed", "population_age_and_department_france_details") }} as pop
left join
    {{ source("seed", "region_department") }} as dep
    on pop.department_code = dep.num_dep
where
    pop.current_year in (2020, 2021, 2022, 2023, 2024, 2025)
    and cast(pop.age as int) between 15 and 25
group by
    date(pop.current_date),
    date(pop.born_date),
    pop.decimal_age,
    pop.department_code,
    pop.department_name,
    dep.academy_name,
    dep.region_name
