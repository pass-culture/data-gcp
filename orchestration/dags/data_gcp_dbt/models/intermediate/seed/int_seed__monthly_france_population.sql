{% set insee_start_year = 2020 %}
{% set current_year = modules.datetime.date.today().year %}
{% set insee_last_valid_year = var("INSEE_DATA_LAST_VALID_YEAR") | int %}

{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
        pre_hook=(
            "select if("
            ~ "extract(year from current_date()) > " ~ insee_last_valid_year|string
            ~ ", error(concat("
            ~ "'INSEE data is stale: current year (', cast(extract(year from current_date()) as string), "
            ~ "' ) is greater than last valid year (', cast(" ~ insee_last_valid_year|string ~ " as string), ').'"
            ~ ")), 1) as runtime_check"
        )
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
    pop.current_year BETWEEN {{ insee_start_year }} AND {{ insee_last_valid_year }}
    and cast(pop.age as int) between 15 and 25
group by
    date(pop.current_date),
    date(pop.born_date),
    pop.decimal_age,
    pop.department_code,
    pop.department_name,
    dep.academy_name,
    dep.region_name
