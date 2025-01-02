with
    eple_infos as (
        select distinct
            eid.institution_id,
            institution_external_id,
            institution_name,
            institution_city,
            institution_department_code,
            institution_region_name as region_name,
            institution_academy_name as institution_academie,
            eid.ministry,
            eid.institution_type,
            eid.macro_institution_type,
            ey.scholar_year,
            ed.educational_deposit_amount as institution_deposit_amount,
            eid.total_students
        from {{ ref("mrt_global__educational_institution") }} eid
        join
            {{ ref("mrt_global__educational_deposit") }} ed
            on ed.educational_institution_id = eid.institution_id
        join {{ ref("educational_year") }} ey on ey.adage_id = ed.educational_year_id
    ),

    eple_bookings as (
        select
            eple_infos.institution_id,
            eple_infos.scholar_year,
            sum(
                case
                    when collective_booking_status != 'CANCELLED'
                    then booking_amount
                    else null
                end
            ) as theoric_amount_spent,
            sum(
                case
                    when collective_booking_status in ('USED', 'REIMBURSED')
                    then booking_amount
                    else null
                end
            ) as real_amount_spent
        from eple_infos
        join
            {{ ref("mrt_global__collective_booking") }} ecbd
            on ecbd.educational_institution_id = eple_infos.institution_id
            and ecbd.scholar_year = eple_infos.scholar_year
        group by 1, 2
    ),

    nb_eleves_educonnectes_per_eple as (
        select
            trim(json_extract(result_content, '$.school_uai'), '"') as school,
            count(distinct edd.user_id) as educonnect_inscriptions,
            count(
                distinct case
                    when date_diff(current_date, deposit_creation_date, day) <= 365
                    then edd.user_id
                    else null
                end
            ) as last_12_months_inscriptions,
            avg(coalesce(total_theoretical_amount_spent, 0)) as avg_spent_per_user,
            safe_divide(
                sum(total_theoretical_amount_spent), sum(deposit_amount)
            ) as pct_spent,
            count(distinct ebd.user_id) as nb_credit_used_students
        from {{ ref("beneficiary_fraud_check") }} bfc
        join {{ ref("mrt_global__deposit") }} edd on edd.user_id = bfc.user_id
        left join
            {{ ref("mrt_global__booking") }} ebd
            on ebd.user_id = edd.user_id
            and not booking_is_cancelled
        where
            type = 'EDUCONNECT'
            and json_extract(result_content, '$.school_uai') is not null
            and edd.deposit_type = 'GRANT_15_17'
        group by 1
    )

select
    eple_infos.institution_id,
    eple_infos.institution_external_id,
    eple_infos.institution_name,
    eple_infos.institution_academie,
    eple_infos.region_name,
    eple_infos.institution_department_code,
    eple_infos.institution_city,
    eple_infos.ministry,
    eple_infos.institution_type,
    eple_infos.macro_institution_type,
    eple_infos.scholar_year,
    institution_deposit_amount,
    theoric_amount_spent,
    safe_divide(
        theoric_amount_spent, institution_deposit_amount
    ) as pct_credit_theoric_amount_spent,
    real_amount_spent,
    safe_divide(
        real_amount_spent, institution_deposit_amount
    ) as pct_credit_real_amount_spent,
    total_students,
    educonnect_inscriptions,
    last_12_months_inscriptions,
    nb_credit_used_students,
    avg_spent_per_user,
    pct_spent as pct_spent_per_user,
    safe_divide(last_12_months_inscriptions, total_students) as pct_beneficiary_students
from eple_infos
left join
    eple_bookings
    on eple_bookings.institution_id = eple_infos.institution_id
    and eple_infos.scholar_year = eple_bookings.scholar_year
left join
    nb_eleves_educonnectes_per_eple
    on eple_infos.institution_external_id = nb_eleves_educonnectes_per_eple.school
