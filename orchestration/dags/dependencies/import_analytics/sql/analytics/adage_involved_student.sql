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
        coalesce(sum(SAFE_CAST(involved_students as FLOAT64)), 0) as involved_students,
        coalesce(sum(SAFE_CAST(total_involved_students as FLOAT64)), 0) as total_involved_students,
    FROM
        `{{ bigquery_clean_dataset }}.adage_involved_student` ais
        LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` ey on SAFE_CAST(ey.adage_id as int) = SAFE_CAST(ais.educational_year_adage_id as int)
        LEFT JOIN `{{ bigquery_clean_dataset }}.institutional_scholar_level` isl on ais.level = isl.level_id
    where
        metric_name = "departements"
    AND 
        involved_students <> "None"
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
)
SELECT
    *
FROM
    involved_students involved