{% set insee_start_year = 2020 %}
{% set current_year = modules.datetime.date.today().year %}
{% set last_valid_year = var("INSEE_DATA_LAST_VALID_YEAR") | int %}

{% if current_year > last_valid_year %}
  {{ exceptions.raise_compiler_error(
      "INSEE data is stale: current year (" ~ current_year ~ 
      ") is greater than last valid year (" ~ last_valid_year ~ ")."
  ) }}
{% endif %}

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
    pop.current_year BETWEEN {{ insee_start_year }} AND {{ last_valid_year }}
    and cast(pop.age as int) between 15 and 25
group by
    date(pop.current_date),
    date(pop.born_date),
    pop.decimal_age,
    pop.department_code,
    pop.department_name,
    dep.academy_name,
    dep.region_name
