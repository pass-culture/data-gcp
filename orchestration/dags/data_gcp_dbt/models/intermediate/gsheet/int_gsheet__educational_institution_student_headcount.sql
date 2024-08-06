SELECT 
    school_year,
    ministry,
    uai as institution_id,
    is_provisional,
    class,
    SAFE_CAST(amount_per_student as FLOAT64) as amount_per_student,
    SAFE_CAST(headcount as FLOAT64) as headcount,
FROM {{ source('raw', 'gsheet_educational_institution_student_headcount') }} 
QUALIFY ROW_NUMBER() over ( PARTITION BY ministry, uai, class ORDER BY CAST(SPLIT(school_year, "-")[0] AS INT) DESC, CAST(is_provisional AS INT) DESC ) = 1