with
    eple_infos as (
        select distinct
            eid.institution_id,
            institution_external_id,
            institution_name,
            institution_city,
            eid.institution_department_code,
            institution_region_name as region_name,
            eid.institution_academy_name as institution_academie,
            eid.institution_epci,
            eid.ministry,
            eid.institution_type,
            eid.macro_institution_type,
            eid.institution_program_name,
            eid.institution_macro_density_label,
            ey.scholar_year,
            ed.educational_deposit_amount as institution_deposit_amount,
            eid.total_students
        from {{ ref("mrt_global__educational_institution") }} as eid
        inner join
            {{ ref("mrt_global__educational_deposit") }} as ed
            on eid.institution_id = ed.institution_id
        inner join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on ed.educational_year_id = ey.educational_year_id
    ),

    eple_bookings as (
        select
            eple_infos.institution_id,
            eple_infos.scholar_year,
            sum(
                case
                    when collective_booking_status != 'CANCELLED' then booking_amount
                end
            ) as theoric_amount_spent,
            sum(
                case
                    when collective_booking_status in ('USED', 'REIMBURSED')
                    then booking_amount
                end
            ) as real_amount_spent,
            coalesce(sum(
                case
                    when collective_booking_status = 'REIMBURSED' then booking_amount
                end
            ),0) as real_amount_reimbursed
        from eple_infos
        inner join
            {{ ref("mrt_global__collective_booking") }} as ecbd
            on eple_infos.institution_id = ecbd.educational_institution_id
            and eple_infos.scholar_year = ecbd.scholar_year
        group by 1, 2
    )


select
    eple_infos.institution_id,
    eple_infos.institution_external_id,
    eple_infos.institution_name,
    eple_infos.institution_academie,
    eple_infos.region_name,
    eple_infos.institution_department_code,
    eple_infos.institution_city,
    eple_infos.institution_epci,
    eple_infos.ministry,
    eple_infos.institution_type,
    eple_infos.macro_institution_type,
    eple_infos.institution_program_name,
    eple_infos.scholar_year,
    institution_deposit_amount,
    theoric_amount_spent,
    real_amount_spent,
    real_amount_reimbursed,
    total_students,
    safe_divide(
        theoric_amount_spent, institution_deposit_amount
    ) as pct_credit_theoric_amount_spent,
    safe_divide(
        real_amount_spent, institution_deposit_amount
    ) as pct_credit_real_amount_spent,
    safe_divide(
        real_amount_reimbursed, institution_deposit_amount
    ) as pct_credit_real_amount_reimbursed
from eple_infos
left join
    eple_bookings
    on eple_infos.institution_id = eple_bookings.institution_id
    and eple_infos.scholar_year = eple_bookings.scholar_year
