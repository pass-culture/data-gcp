with users_grouped_by_institution AS (
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
        ei.institution_departement_code
    ) as institution_department_code,
    institution_program.institution_program_name AS institution_program_name,
    ugi.total_credited_beneficiaries,
from {{ source('raw', 'applicative_database_educational_institution') }} AS ei
left join {{ ref('int_applicative__institution_program') }} AS institution_program
    on ei.educational_institution_id = institution_program.institution_id
left join users_grouped_by_institution as ugi on ugi.institution_external_id = ei.institution_id
