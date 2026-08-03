with
    last_day_of_month as (
        select
            date_trunc(date, month) as partition_month,
            max(date) as last_date,
            max(cast(adage_id as int)) as last_adage_id
        from {{ ref("adage_involved_student") }}
        group by date_trunc(date, month)
    ),

    coverage_aggregated as (
        select
            involved.scholar_year,
            rd.region_name,
            rd.academy_name,
            involved.department_code,
            rd.dep_name as department_name,
            date_trunc(involved.date, month) as partition_month,
            coalesce(rd.region_code, -1) as region_code,
            sum(involved.total_involved_students) as total_eligible_students,
            sum(involved.involved_students) as total_engaged_students
        from {{ ref("adage_involved_student") }} as involved
        inner join
            last_day_of_month
            on involved.date = last_day_of_month.last_date
            and date_trunc(involved.date, month) = last_day_of_month.partition_month
            and last_day_of_month.last_adage_id = cast(involved.adage_id as int)
        left join
            {{ source("seed", "region_department") }} as rd
            on involved.department_code = rd.num_dep
        where involved.department_code != '-1'
        group by
            involved.scholar_year,
            rd.region_name,
            rd.academy_name,
            involved.department_code,
            rd.dep_name,
            date_trunc(involved.date, month),
            coalesce(rd.region_code, -1)
    )

select
    coverage_aggregated.partition_month,
    coverage_aggregated.scholar_year,
    coverage_aggregated.region_code,
    coverage_aggregated.region_name,
    coverage_aggregated.academy_name,
    coverage_aggregated.department_code,
    coverage_aggregated.department_name,
    coverage_aggregated.total_eligible_students,
    coverage_aggregated.total_engaged_students
from coverage_aggregated
