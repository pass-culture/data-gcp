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
    u.user_is_in_education,
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
    d.deposit_reform_category,
    d.deposit_rank_asc,
    d.deposit_rank_desc,
    d.last_recredit_date,
    d.total_recredit,
    d.total_recredit_amount,
    d.total_theoretical_amount_spent,
    d.total_actual_amount_spent,
    d.total_theoretical_amount_spent_in_digital_goods,
    d.total_non_cancelled_individual_bookings,
    d.total_diversity_score,
    d.first_individual_booking_date,
    d.last_individual_booking_date,
    u.user_birth_date,
    date_diff(
        current_date(), cast(d.deposit_creation_date as date), day
    ) as deposit_seniority,
    date_diff(
        cast(d.deposit_creation_date as date), cast(u.user_creation_date as date), day
    ) as days_between_user_creation_and_deposit_creation
from {{ ref("int_global__deposit") }} as d
inner join {{ ref("int_global__user_beneficiary") }} as u on d.user_id = u.user_id
left join
    {{ ref("int_applicative__action_history") }} as ah
    on d.user_id = ah.user_id
    and ah.action_history_rk = 1
where (u.user_is_active or ah.action_history_reason = 'upon user request')
