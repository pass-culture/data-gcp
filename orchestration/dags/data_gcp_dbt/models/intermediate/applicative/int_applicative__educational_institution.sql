with
    deposit_grouped_by_institution as (
        select
            institution_id,
            max(case when deposit_rank_asc = 1 then ministry end) as ministry,
            max(
                case
                    when deposit_rank_asc = 1 then educational_deposit_creation_date
                end
            ) as first_deposit_creation_date,
            max(
                case when is_current_deposit then educational_deposit_amount end
            ) as current_deposit_amount,
            max(
                case when is_current_deposit then educational_deposit_creation_date end
            ) as current_deposit_creation_date,
            sum(
                case when is_current_scholar_year then educational_deposit_amount end
            ) as total_current_scholar_year_deposit_amount,
            sum(
                case
                    when calendar_year = extract(year from current_date)
                    then educational_deposit_amount
                end
            ) as total_current_calendar_year_deposit_amount,
            sum(educational_deposit_amount) as total_deposit_amount,
            count(*) as total_deposits
        from {{ ref("int_applicative__educational_deposit") }}
        group by institution_id
    ),

    users_grouped_by_institution as (
        select
            regexp_extract(
                result_content, '"school_uai": "(.*?)",'
            ) as institution_external_id,
            count(distinct user_id) as total_credited_beneficiaries
        from {{ ref("int_applicative__beneficiary_fraud_check") }}
        where
            type = 'EDUCONNECT'
            and regexp_extract(result_content, '"school_uai": "(.*?)",') is not null
            and (user_is_active or action_history_reason = 'upon user request')
        group by institution_external_id
    )

select
    ei.educational_institution_id,
    ei.institution_id,
    ei.institution_name,
    ei.institution_type,
    eil_loc.institution_city,
    eil_loc.institution_city_code,
    eil_loc.institution_epci,
    eil_loc.institution_epci_code,
    eil_loc.institution_density_label,
    eil_loc.institution_macro_density_label,
    eil_loc.institution_density_level,
    eil_loc.institution_latitude,
    eil_loc.institution_longitude,
    eil_loc.institution_academy_name,
    eil_loc.institution_region_name,
    eil_loc.institution_in_qpv,
    eil_loc.institution_department_code,
    eil_loc.institution_department_name,
    eil_loc.institution_internal_iris_id,
    eil_loc.institution_postal_code,
    dgi.ministry,
    dgi.first_deposit_creation_date,
    dgi.current_deposit_amount,
    dgi.current_deposit_creation_date,
    dgi.total_current_scholar_year_deposit_amount,
    dgi.total_current_calendar_year_deposit_amount,
    dgi.total_deposit_amount,
    dgi.total_deposits,
    institution_program.institution_program_name,
    ugi.total_credited_beneficiaries
from {{ source("raw", "applicative_database_educational_institution") }} as ei
left join
    {{ ref("int_applicative__institution_program") }} as institution_program
    on ei.educational_institution_id = institution_program.institution_id
left join
    deposit_grouped_by_institution as dgi
    on ei.educational_institution_id = dgi.institution_id
left join
    users_grouped_by_institution as ugi
    on ei.institution_id = ugi.institution_external_id
left join
    {{ ref("int_geo__educational_institution_location") }} as eil_loc
    on ei.educational_institution_id = eil_loc.educational_institution_id
