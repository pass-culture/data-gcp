with
    involved_students as (
        select
            date(execution_date) as date,
            case
                when metric_id = "02A"
                then "2A"
                when metric_id = "02B"
                then "2B"
                when upper(metric_id) = "TOTAL"
                then "-1"
                when upper(metric_id) like "%97%"
                then right(metric_id, 3)
                else right(metric_id, 2)
            end as department_code,
            ey.educational_year_id,
            ey.educational_year_beginning_date,
            ey.educational_year_expiration_date,
            ey.adage_id,
            ey.scholar_year,
            ais.level,
            coalesce(sum(safe_cast(institutions as float64)), 0) as institutions,
            coalesce(
                sum(safe_cast(total_institutions as float64)), 0
            ) as total_institutions
        from {{ source("clean", "adage_involved_student") }} as ais
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on safe_cast(ey.adage_id as int)
            = safe_cast(ais.educational_year_adage_id as int)
        where metric_name = "departements"
        group by 1, 2, 3, 4, 5, 6, 7, 8
    )

select
    involved.date,
    involved.department_code,
    involved.educational_year_id,
    involved.educational_year_beginning_date,
    involved.educational_year_expiration_date,
    involved.adage_id,
    involved.scholar_year,
    avg(institutions) as institutions,
    avg(total_institutions) as total_institutions
from involved_students as involved
where not is_nan(institutions)
group by 1, 2, 3, 4, 5, 6, 7
