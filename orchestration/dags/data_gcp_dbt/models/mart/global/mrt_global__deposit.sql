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
    u.user_density_level,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_is_priority_public,
    u.user_department_code,
    u.user_department_name,
    u.user_age,
    u.user_creation_date,
    d.deposit_source,
    d.deposit_creation_date,
    d.deposit_update_date,
    d.deposit_expiration_date,
    d.deposit_type,
    d.deposit_rank_asc,
    d.deposit_rank_desc,
    d.total_theoretical_amount_spent,
    d.total_actual_amount_spent,
    d.total_theoretical_amount_spent_in_digital_goods,
    d.total_non_cancelled_individual_bookings,
    d.first_individual_booking_date,
    d.last_individual_booking_date,
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
from {{ ref('int_global__deposit') }} as d
    inner join {{ ref('int_global__user') }} as u on u.user_id = d.user_id
    left join {{ ref('int_applicative__action_history') }} as ah on ah.user_id = d.user_id and ah.action_history_rk = 1
where
    (
        u.user_is_active
        or ah.action_history_reason = 'upon user request'
    )
