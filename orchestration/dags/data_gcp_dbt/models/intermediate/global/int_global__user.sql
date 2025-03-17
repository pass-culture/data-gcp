with
    deposit_grouped_by_user as (
        select
            user_id,
            min(deposit_creation_date) as first_deposit_creation_date,
            min(deposit_amount) as first_deposit_amount,
            max(deposit_amount) as last_deposit_amount,
            max(deposit_expiration_date) as last_deposit_expiration_date,
            sum(deposit_amount) as total_deposit_amount,
            sum(
                total_non_cancelled_individual_bookings
            ) as total_non_cancelled_individual_bookings,
            sum(total_non_cancelled_duo_bookings) as total_non_cancelled_duo_bookings,
            sum(total_free_bookings) as total_free_bookings,
            sum(total_actual_amount_spent) as total_actual_amount_spent,
            sum(total_theoretical_amount_spent) as total_theoretical_amount_spent,
            min(first_individual_booking_date) as first_individual_booking_date,
            max(last_individual_booking_date) as last_individual_booking_date,
            sum(
                case when deposit_type = "GRANT_18" then total_subcategory_booked end
            ) as total_grant_18_subcategory_booked,
            sum(
                case when deposit_type = "GRANT_15_17" then total_subcategory_booked end
            ) as total_grant_15_17_subcategory_booked,
            sum(
                total_theoretical_physical_goods_amount_spent
            ) as total_theoretical_physical_goods_amount_spent,
            sum(
                total_theoretical_digital_goods_amount_spent
            ) as total_theoretical_digital_goods_amount_spent,
            sum(
                total_theoretical_outings_amount_spent
            ) as total_theoretical_outings_amount_spent,
            sum(total_diversity_score) as total_diversity_score,
            max(first_booking_type) as first_booking_type,
            max(first_paid_booking_type) as first_paid_booking_type,
            min(first_paid_booking_date) as first_paid_booking_date,
            sum(
                case when deposit_rank_desc = 1 then total_actual_amount_spent end
            ) as total_deposit_actual_amount_spent,
            sum(
                case
                    when deposit_rank_desc = 1
                    then total_theoretical_amount_spent_in_digital_goods
                end
            ) as total_last_deposit_digital_goods_amount_spent,
            sum(
                case when deposit_rank_desc = 1 then total_theoretical_amount_spent end
            ) as total_last_deposit_theoretical_amount_spent,
            min(deposit_creation_date) as user_activation_date,
            max(last_recredit_date) as last_recredit_date,
            sum(total_recredit) as total_recredit,
            sum(total_recredit_amount) as total_recredit_amount,
            max(
                case when deposit_rank_asc = 1 then deposit_type end
            ) as first_deposit_type,
            max(
                case when deposit_rank_desc = 1 then deposit_type end
            ) as current_deposit_type,
            max(
                case when deposit_rank_asc = 1 then deposit_reform_category end
            ) as user_first_deposit_reform_category,
            max(
                case when deposit_rank_desc = 1 then deposit_reform_category end
            ) as user_current_deposit_reform_category
        from {{ ref("int_global__deposit") }}
        group by user_id
    )

select
    u.user_id,
    u.user_activity,
    u.user_civility,
    u.user_school_type,
    u.user_is_active,
    u.user_age,
    u.user_creation_date,
    u.user_birth_date,
    u.user_has_enabled_marketing_email,
    u.user_has_enabled_marketing_push,
    u.user_subscribed_themes,
    u.is_theme_subscribed,
    ui.user_iris_internal_id,
    ui.user_department_code,
    ui.user_postal_code,
    ui.user_region_name,
    ui.user_department_name,
    ui.user_city,
    ui.user_epci,
    ui.user_academy_name,
    ui.user_density_label,
    ui.user_macro_density_label,
    ui.user_density_level,
    ui.user_city_code,
    ui.user_is_in_qpv,
    u.user_humanized_id,
    dgu.first_deposit_creation_date,
    dgu.total_deposit_amount,
    dgu.user_activation_date,
    dgu.first_individual_booking_date,
    dgu.total_actual_amount_spent,
    dgu.total_theoretical_amount_spent,
    dgu.total_theoretical_digital_goods_amount_spent,
    dgu.total_theoretical_physical_goods_amount_spent,
    dgu.total_theoretical_outings_amount_spent,
    dgu.total_last_deposit_digital_goods_amount_spent,
    dgu.total_last_deposit_theoretical_amount_spent,
    dgu.total_deposit_actual_amount_spent,
    dgu.total_diversity_score,
    dgu.last_deposit_amount,
    dgu.last_individual_booking_date as last_booking_date,
    dgu.first_booking_type,
    dgu.first_paid_booking_type,
    dgu.total_grant_18_subcategory_booked,
    dgu.total_grant_15_17_subcategory_booked,
    ah.action_history_reason as user_suspension_reason,
    dgu.first_deposit_amount,
    dgu.last_deposit_expiration_date,
    dgu.last_recredit_date,
    dgu.total_recredit,
    dgu.total_recredit_amount,
    dgu.first_deposit_type,
    dgu.current_deposit_type,
    dgu.user_first_deposit_reform_category,
    dgu.user_current_deposit_reform_category,
    coalesce(
        u.user_activity = "Chômeur, En recherche d'emploi", false
    ) as user_is_unemployed,
    coalesce(
        u.user_activity in ("Collégien", "Etudiant", "Lycéen", "Apprenti", "Alternant"),
        false
    ) as user_is_in_education,
    coalesce(
        (
            (ui.qpv_name is not null)
            or (
                u.user_activity
                not in ("Collégien", "Etudiant", "Lycéen", "Apprenti", "Alternant")
            )
            or (ui.user_macro_density_label = "rural")
        ),
        false
    ) as user_is_priority_public,
    coalesce(
        dgu.total_non_cancelled_individual_bookings, 0
    ) as total_non_cancelled_individual_bookings,
    coalesce(dgu.total_free_bookings, 0) as total_free_bookings,
    coalesce(
        dgu.total_non_cancelled_duo_bookings, 0
    ) as total_non_cancelled_duo_bookings,
    dgu.last_deposit_amount
    - dgu.total_last_deposit_theoretical_amount_spent
    as total_theoretical_remaining_credit,
    date_diff(
        dgu.first_individual_booking_date, dgu.first_deposit_creation_date, day
    ) as days_between_activation_date_and_first_booking_date,
    date_diff(
        dgu.first_individual_booking_date, dgu.first_deposit_creation_date, day
    ) as days_between_activation_date_and_first_booking_paid,
    coalesce(
        (
            timestamp(dgu.last_deposit_expiration_date) >= current_timestamp()
            and coalesce(dgu.total_deposit_actual_amount_spent, 0)
            < dgu.last_deposit_amount
        )
        and u.user_is_active,
        false
    ) as user_is_current_beneficiary
from {{ ref("int_applicative__user") }} as u
left join
    {{ ref("int_applicative__action_history") }} as ah
    on u.user_id = ah.user_id
    and ah.action_history_rk = 1
left join {{ ref("int_geo__user_location") }} as ui on u.user_id = ui.user_id
left join deposit_grouped_by_user as dgu on u.user_id = dgu.user_id
