WITH deposit_grouped_by_institution AS (
SELECT institution_id,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN ministry END) AS ministry,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN deposit_creation_date END) AS first_deposit_creation_date,
    MAX(CASE WHEN is_current_deposit THEN educational_deposit_amount END) AS current_deposit_amount,
    MAX(CASE WHEN is_current_deposit THEN deposit_creation_date END) AS current_deposit_creation_date,
    SUM(educational_deposit_amount) AS total_deposit_amount,
    COUNT(*) AS total_deposits,
FROM {{ ref('int_applicative__educational_deposit') }} AS ed
GROUP BY institution_id
),

 users_grouped_by_institution AS (
select REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') AS institution_external_id,
    count(DISTINCT user_id) AS total_credited_beneficiaries
from {{ ref('int_applicative__beneficiary_fraud_check') }}
where type = 'EDUCONNECT'
    and REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') IS NOT NULL
    and (user_is_active OR action_history_reason = 'upon user request')
group by institution_external_id
)

select
    ei.educational_institution_id,
    ei.institution_id,
    ei.institution_name,
    ei.institution_postal_code,
    ei.institution_type,
    coalesce(
        case
            when ei.institution_postal_code = '97150' then '978'
            when substring(ei.institution_postal_code, 0, 2) = '97' then substring(ei.institution_postal_code, 0, 3)
            when substring(ei.institution_postal_code, 0, 2) = '98' then substring(ei.institution_postal_code, 0, 3)
            when substring(ei.institution_postal_code, 0, 3) in ('200', '201', '209', '205') then '2A'
            when substring(ei.institution_postal_code, 0, 3) in ('202', '206') then '2B'
            ELSE substring(ei.institution_postal_code, 0, 2)
        END,
        ei.institution_department_code
    ) as institution_department_code,
    dgi.ministry,
    dgi.first_deposit_creation_date,
    dgi.current_deposit_amount,
    dgi.current_deposit_creation_date,
    dgi.total_deposit_amount,
    dgi.total_deposits,
    institution_program.institution_program_name AS institution_program_name,
    ugi.total_credited_beneficiaries,
from {{ source('raw', 'applicative_database_educational_institution') }} AS ei
left join {{ ref('int_applicative__institution_program') }} AS institution_program
    on ei.educational_institution_id = institution_program.institution_id
left join deposit_grouped_by_institution AS dgi ON dgi.institution_id = ei.educational_institution_id
left join users_grouped_by_institution as ugi on ugi.institution_external_id = ei.institution_id
