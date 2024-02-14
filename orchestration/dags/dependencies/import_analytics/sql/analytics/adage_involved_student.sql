WITH involved_students AS (
    SELECT
        date(execution_date) as date,
        CASE
            WHEN metric_id = "02A" THEN "2A"
            WHEN metric_id = "02B" THEN "2B"
            WHEN upper(metric_id) = "TOTAL" THEN "-1"
            WHEN upper(metric_id) LIKE "%97%" THEN RIGHT(metric_id, 3)
            ELSE RIGHT(metric_id, 2)
        END as department_code,
        ey.educational_year_id,
        ey.educational_year_beginning_date,
        ey.educational_year_expiration_date,
        ey.adage_id,
        ey.scholar_year,
        isl.level_id,
        isl.level_code,
        CASE 
            WHEN isl.level_code in ("6EME", "5EME", "4EME", "3EME", "6E SEGPA", "5E SEGPA", "4E SEGPA", "3E SEGPA") THEN "Collège" 
            WHEN isl.level_code in ("2NDE G-T", "1ERE G-T", "TERM G-T", "CAP 1 AN", "1CAP2", "2CAP2", "2CAP3", "3CAP3", "2NDE PRO", "1ERE PRO", "TLEPRO" ) THEN "Lycée"
            WHEN isl.level_code = "MLDS" THEN "MLDS"
            ELSE isl.level_code 
        END AS niveau_education_macro,
        coalesce(sum(SAFE_CAST(involved_students as FLOAT64)), 0) as involved_students,
        coalesce(sum(SAFE_CAST(total_involved_students as FLOAT64)), 0) as total_involved_students,
    FROM
        `{{ bigquery_clean_dataset }}.adage_involved_student` ais
        LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` ey on SAFE_CAST(ey.adage_id as int) = SAFE_CAST(ais.educational_year_adage_id as int)
        LEFT JOIN `{{ bigquery_clean_dataset }}.institutional_scholar_level` isl on ais.level = isl.level_id
    where
        metric_name = "departements"
    GROUP BY
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
SELECT
    *
FROM
    involved_students involved
WHERE NOT Is_NAN(involved_students)