with
    institutional_scholar_level as (
        select n_ms4_id as level_id, n_ms4_cod as level_code, n_ms4_lib as level_lib
        from {{ source("seed", "institutional_scholar_level") }}
    ),

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
            isl.level_id,
            isl.level_code,
            case
                when
                    isl.level_code in (
                        "6EME",
                        "5EME",
                        "4EME",
                        "3EME",
                        "6E SEGPA",
                        "5E SEGPA",
                        "4E SEGPA",
                        "3E SEGPA"
                    )
                then "Collège"
                when
                    isl.level_code in (
                        "2NDE G-T",
                        "1ERE G-T",
                        "TERM G-T",
                        "CAP 1 AN",
                        "1CAP2",
                        "2CAP2",
                        "2CAP3",
                        "3CAP3",
                        "2NDE PRO",
                        "1ERE PRO",
                        "TLEPRO"
                    )
                then "Lycée"
                when isl.level_code = "MLDS"
                then "MLDS"
                else isl.level_code
            end as level_macro,
            coalesce(
                sum(safe_cast(involved_students as float64)), 0
            ) as involved_students,
            coalesce(
                sum(safe_cast(total_involved_students as float64)), 0
            ) as total_involved_students
        from {{ source("clean", "adage_involved_student") }} as ais
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on safe_cast(ey.adage_id as int)
            = safe_cast(ais.educational_year_adage_id as int)
        left join institutional_scholar_level as isl on ais.level = isl.level_id
        where metric_name = "departements"
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )

select *
from involved_students as involved
where not is_nan(involved.involved_students)
