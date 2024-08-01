with involved_students as (
    select
        date(execution_date) as date,
        case
            when metric_id = "02A" then "2A"
            when metric_id = "02B" then "2B"
            when upper(metric_id) = "TOTAL" then "-1"
            when upper(metric_id) like "%97%" then right(metric_id, 3)
            else right(metric_id, 2)
        end as department_code,
        ey.educational_year_id,
        ey.educational_year_beginning_date,
        ey.educational_year_expiration_date,
        ey.adage_id,
        ey.scholar_year,
        isl.level_id,
        isl.level_code,
        case
            when isl.level_code in ("6EME", "5EME", "4EME", "3EME", "6E SEGPA", "5E SEGPA", "4E SEGPA", "3E SEGPA") then "Collège"
            when isl.level_code in ("2NDE G-T", "1ERE G-T", "TERM G-T", "CAP 1 AN", "1CAP2", "2CAP2", "2CAP3", "3CAP3", "2NDE PRO", "1ERE PRO", "TLEPRO") then "Lycée"
            when isl.level_code = "MLDS" then "MLDS"
            else isl.level_code
        end as level_macro,
        coalesce(sum(safe_cast(involved_students as FLOAT64)), 0) as involved_students,
        coalesce(sum(safe_cast(total_involved_students as FLOAT64)), 0) as total_involved_students
    from {{ source('clean','adage_involved_student') }} ais
        left join {{ ref('educational_year') }} ey on safe_cast(ey.adage_id as INT) = safe_cast(ais.educational_year_adage_id as INT)
        left join {{ source('clean','institutional_scholar_level') }} isl on ais.level = isl.level_id
    where
        metric_name = "departements"
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
)

select *
from
    involved_students involved
where not is_nan(involved_students)
