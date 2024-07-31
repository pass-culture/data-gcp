with bookings_grouped_by_deposit as (
    select
        deposit_id,
        SUM(case when booking_is_used then booking_amount * booking_quantity end) as total_actual_amount_spent,
        SUM(booking_amount * booking_quantity) as total_theoretical_amount_spent,
        SUM(case when digital_goods and offer_url is not NULL
                then booking_amount * booking_quantity
        end) as total_theoretical_amount_spent_in_digital_goods,
        MIN(booking_creation_date) as first_individual_booking_date,
        MAX(booking_creation_date) as last_individual_booking_date,
        COUNT(distinct booking_id) as total_non_cancelled_individual_bookings
    from {{ ref('mrt_global__booking') }}
    where not booking_is_cancelled
    group by deposit_id
)

select
    d.deposit_id,
    d.deposit_amount,
    d.user_id,
    u.user_civility,
    u.user_region_name,
    u.user_postal_code,
    u.user_city,
    u.user_epci,
    u.user_academy_name,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_department_code,
    u.user_age,
    u.user_creation_date,
    d.deposit_source,
    d.deposit_creation_date,
    d.deposit_update_date,
    d.deposit_expiration_date,
    d.deposit_type,
    d.deposit_rank_asc,
    d.deposit_rank_desc,
    bgd.total_theoretical_amount_spent,
    bgd.total_actual_amount_spent,
    bgd.total_theoretical_amount_spent_in_digital_goods,
    bgd.total_non_cancelled_individual_bookings,
    bgd.first_individual_booking_date,
    bgd.last_individual_booking_date,
    DATE_DIFF(
        CURRENT_DATE(),
        CAST(d.deposit_creation_date as DATE),
        day
    ) as deposit_seniority,
    DATE_DIFF(
        CAST(d.deposit_creation_date as DATE),
        CAST(u.user_creation_date as DATE),
        day
    ) as days_between_user_creation_and_deposit_creation,
    u.user_birth_date
from {{ ref('int_applicative__deposit') }} as d
    left join bookings_grouped_by_deposit as bgd on bgd.deposit_id = d.deposit_id
    inner join {{ ref('int_applicative__user') }} as u on u.user_id = d.user_id
    left join {{ ref('int_applicative__action_history') }} as ah on ah.user_id = d.user_id and ah.action_history_rk = 1
where
    (
        u.user_is_active
        or ah.action_history_reason = 'upon user request'
    )
