WITH involved_students AS (
    SELECT
        date(execution_date) as date,
        CASE
            WHEN metric_id = "02A" THEN "20"
            WHEN metric_id = "02B" THEN "20"
            WHEN upper(metric_id) = "TOTAL" THEN "-1"
            ELSE RIGHT(metric_id, 2)
        END as department_code,
        ey.educational_year_id,
        ey.educational_year_beginning_date,
        ey.educational_year_expiration_date,
        ey.adage_id,
        sum(CAST(involved_students as FLOAT64)) as involved_students,
        sum(cast(institutions as FLOAT64)) as institutions,
        sum(cast(total_involved_students as FLOAT64)) as total_involved_students,
        sum(cast(total_institutions as FLOAT64)) as total_institutions,
    FROM
        `{{ bigquery_clean_dataset }}.adage_involved_student` ais
        LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` ey on cast(ey.adage_id as int) = cast(ais.educational_year_adage_id as int)
    where
        metric_name = "departements"
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
)
SELECT
    *
FROM
    involved_students involved