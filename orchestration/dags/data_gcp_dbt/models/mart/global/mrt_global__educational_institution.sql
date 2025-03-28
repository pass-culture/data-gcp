with
    collective_booking_grouped_by_institution as (

        select
            educational_institution_id,
            sum(collective_stock_number_of_tickets) as total_tickets,
            sum(
                case
                    when is_current_year_booking then collective_stock_number_of_tickets
                end
            ) as total_current_year_tickets,
            sum(
                case
                    when collective_booking_status in ('USED', 'REIMBURSED')
                    then booking_amount
                    else null
                end
            ) as total_collective_real_revenue,
            sum(
                case
                    when
                        (
                            collective_booking_status in ('USED', 'REIMBURSED')
                            and is_current_year_booking
                        )
                    then booking_amount
                    else null
                end
            ) as total_current_year_collective_real_revenue,
            sum(booking_amount) as total_collective_theoretic_revenue,
            sum(
                case when is_current_year_booking then booking_amount end
            ) as total_current_year_collective_theoretic_revenue,
            count(
                case when is_used_collective_booking then collective_booking_id end
            ) as total_used_collective_bookings,
            count(
                case
                    when is_used_collective_booking and is_current_year_booking
                    then collective_booking_id
                end
            ) as total_current_year_used_collective_bookings,
            count(
                distinct collective_booking_id
            ) as total_non_cancelled_collective_bookings,
            count(
                distinct case
                    when is_current_year_booking then collective_booking_id
                end
            ) as total_current_year_non_cancelled_collective_bookings,
            max(
                case
                    when collective_booking_rank_asc = 1
                    then collective_booking_creation_date
                end
            ) as first_booking_date,
            max(
                case
                    when collective_booking_rank_desc = 1
                    then collective_booking_creation_date
                end
            ) as last_booking_date,
            max(
                case
                    when collective_booking_rank_desc = 1
                    then collective_offer_subcategory_id
                end
            ) as last_category_booked
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status != 'CANCELLED'
        group by educational_institution_id

    ),

    educational_institution_student_headcount as (
        select institution_id, sum(headcount) as total_students,
        from {{ ref("int_gsheet__educational_institution_student_headcount") }}
        group by institution_id
    )

select
    ei.educational_institution_id as institution_id,
    ei.institution_id as institution_external_id,
    ei.institution_name as institution_name,
    ei.ministry,
    ei.institution_type,
    ei.institution_program_name,
    ei.first_deposit_creation_date,
    coalesce(ei.current_deposit_amount, 0) as current_deposit_amount,
    ei.current_deposit_creation_date,
    coalesce(ei.total_deposit_amount, 0) as total_deposit_amount,
    coalesce(ei.total_deposits, 0) as total_deposits,
    cb.first_booking_date,
    cb.last_booking_date,
    cb.last_category_booked,
    coalesce(
        cb.total_non_cancelled_collective_bookings, 0
    ) as total_non_cancelled_collective_bookings,
    coalesce(
        cb.total_current_year_non_cancelled_collective_bookings, 0
    ) as total_current_year_non_cancelled_collective_bookings,
    coalesce(
        cb.total_collective_theoretic_revenue, 0
    ) as total_collective_theoretic_revenue,
    coalesce(
        cb.total_current_year_collective_theoretic_revenue, 0
    ) as total_current_year_collective_theoretic_revenue,
    coalesce(cb.total_used_collective_bookings, 0) as total_used_collective_bookings,
    coalesce(
        cb.total_current_year_used_collective_bookings, 0
    ) as total_current_year_used_collective_bookings,
    coalesce(cb.total_collective_real_revenue, 0) as total_collective_real_revenue,
    coalesce(
        cb.total_current_year_collective_real_revenue, 0
    ) as total_current_year_collective_real_revenue,
    safe_divide(
        cb.total_current_year_collective_real_revenue, ei.current_deposit_amount
    ) as ratio_current_credit_utilization,
    coalesce(cb.total_tickets, 0) as total_tickets,
    coalesce(cb.total_current_year_tickets, 0) as total_current_year_tickets,
    coalesce(ei.total_credited_beneficiaries, 0) as total_credited_beneficiaries,
    safe_divide(
        ei.total_credited_beneficiaries, sh.total_students
    ) as ratio_beneficiary_students,
    sh.total_students,
    ei.institution_type as marco_institution_type,
    location_info.institution_city,
    location_info.institution_epci,
    location_info.institution_density_label,
    location_info.institution_macro_density_label,
    location_info.institution_density_level,
    location_info.institution_latitude,
    location_info.institution_longitude,
    location_info.institution_academy_name,
    location_info.institution_region_name,
    location_info.institution_in_qpv,
    location_info.institution_department_code,
    location_info.institution_department_name,
    location_info.institution_internal_iris_id,
    location_info.institution_postal_code
from {{ ref("int_applicative__educational_institution") }} as ei
left join
    collective_booking_grouped_by_institution as cb
    on cb.educational_institution_id = ei.educational_institution_id
left join
    educational_institution_student_headcount as sh
    on sh.institution_id = ei.institution_id
left join
    {{ ref("int_geo__institution_location") }} as location_info
    on ei.institution_id = location_info.institution_id
