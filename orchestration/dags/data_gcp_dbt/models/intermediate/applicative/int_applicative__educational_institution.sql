WITH educational_institution_student_headcount AS (
    SELECT
        institution_id,
        sum(headcount) as total_students,
        avg(amount_per_student) as average_outing_budget_per_student,
    FROM {{ ref("int_gsheet__educational_institution_student_headcount") }} 
    GROUP BY institution_id
)

select
    educational_institution_id,
    institution_id,
    institution_city,
    institution_name,
    institution_postal_code,
    institution_type,
    COALESCE(
        case
            when institution_postal_code = '97150' then '978'
            when SUBSTRING(institution_postal_code, 0, 2) = '97' then SUBSTRING(institution_postal_code, 0, 3)
            when SUBSTRING(institution_postal_code, 0, 2) = '98' then SUBSTRING(institution_postal_code, 0, 3)
            when SUBSTRING(institution_postal_code, 0, 3) in ('200', '201', '209', '205') then '2A'
            when SUBSTRING(institution_postal_code, 0, 3) in ('202', '206') then '2B'
            else SUBSTRING(institution_postal_code, 0, 2)
        end,
        institution_departement_code
    ) as institution_departement_code,
    total_students,
    average_outing_budget_per_student,
    
from {{ source('raw', 'applicative_database_educational_institution') }}
left join educational_institution_student_headcount using (institution_id)