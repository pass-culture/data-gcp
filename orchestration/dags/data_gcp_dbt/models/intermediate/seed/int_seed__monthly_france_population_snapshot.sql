
select
    date(pop.current_date) as snapshot_month,
    date(pop.born_date) as population_birth_month,
    pop.population_decimal_age,
    pop.department_code,
    pop.department_name,
    pop.academy_name,
    dep.region_name,
    sum(pop.population) as population
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
    pop.academy_name,
    dep.region_name
