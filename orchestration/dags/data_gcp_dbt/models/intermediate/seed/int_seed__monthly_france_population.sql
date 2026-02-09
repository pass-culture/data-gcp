{% set insee_start_year = 2020 %}
{% set current_year = modules.datetime.date.today().year %}
{% set insee_last_valid_year = var("INSEE_DATA_LAST_VALID_YEAR") | int %}
{% set current_date = modules.datetime.date.today() %}
{% set fail_date = modules.datetime.date(insee_last_valid_year + 1, 1, 15) %}  -- The date after which the INSEE data is considered stale (January 15th of the year following the last valid year)

{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
        pre_hook=(
            "select if("
            ~ "'"
            ~ current_date
            ~ "' > '"
            ~ fail_date
            ~ "', "
            ~ "error(concat("
            ~ "'INSEE data is stale: current date (', cast(current_date as string), "
            ~ "') is after the allowed limit of ', cast('"
            ~ fail_date
            ~ "' as string), '. ', "
            ~ "'Please update the INSEE seed data for this year and increment INSEE_DATA_LAST_VALID_YEAR in dbt_project.yml vars.'"
            ~ ")), 1) as runtime_check"
        ),
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
    pop.current_year between {{ insee_start_year }} and {{ insee_last_valid_year }}
    and cast(pop.age as int) between 15 and 25
group by
    date(pop.current_date),
    date(pop.born_date),
    pop.decimal_age,
    pop.department_code,
    pop.department_name,
    dep.academy_name,
    dep.region_name
