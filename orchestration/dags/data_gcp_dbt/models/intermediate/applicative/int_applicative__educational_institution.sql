with
    deposit_grouped_by_institution as (
        select
            institution_id,
            max(case when deposit_rank_asc = 1 then ministry end) as ministry,
            max(
                case when deposit_rank_asc = 1 then deposit_creation_date end
            ) as first_deposit_creation_date,
            max(
                case when is_current_deposit then educational_deposit_amount end
            ) as current_deposit_amount,
            max(
                case when is_current_deposit then deposit_creation_date end
            ) as current_deposit_creation_date,
            sum(educational_deposit_amount) as total_deposit_amount,
            count(*) as total_deposits,
        from {{ ref("int_applicative__educational_deposit") }} as ed
        group by institution_id
    ),

    users_grouped_by_institution as (
        select
            regexp_extract(
                result_content, '"school_uai": \"(.*?)\",'
            ) as institution_external_id,
            count(distinct user_id) as total_credited_beneficiaries
        from {{ ref("int_applicative__beneficiary_fraud_check") }}
        where
            type = 'EDUCONNECT'
            and regexp_extract(result_content, '"school_uai": \"(.*?)\",') is not null
            and (user_is_active or action_history_reason = 'upon user request')
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
            when ei.institution_postal_code = '97150'
            then '978'
            when substring(ei.institution_postal_code, 0, 2) = '97'
            then substring(ei.institution_postal_code, 0, 3)
            when substring(ei.institution_postal_code, 0, 2) = '98'
            then substring(ei.institution_postal_code, 0, 3)
            when
                substring(ei.institution_postal_code, 0, 3)
                in ('200', '201', '209', '205')
            then '2A'
            when substring(ei.institution_postal_code, 0, 3) in ('202', '206')
            then '2B'
            else substring(ei.institution_postal_code, 0, 2)
        end,
        ei.institution_department_code
    ) as institution_department_code,
    dgi.ministry,
    dgi.first_deposit_creation_date,
    dgi.current_deposit_amount,
    dgi.current_deposit_creation_date,
    dgi.total_deposit_amount,
    dgi.total_deposits,
    institution_program.institution_program_name as institution_program_name,
    ugi.total_credited_beneficiaries,
from {{ source("raw", "applicative_database_educational_institution") }} as ei
left join
    {{ ref("int_applicative__institution_program") }} as institution_program
    on ei.educational_institution_id = institution_program.institution_id
left join
    deposit_grouped_by_institution as dgi
    on dgi.institution_id = ei.educational_institution_id
left join
    users_grouped_by_institution as ugi
    on ugi.institution_external_id = ei.institution_id
