select
    u.user_id,
    u.user_creation_date,
    u.user_has_enabled_marketing_email,
    u.user_activity,
    u.user_civility,
    u.user_birth_date,
    u.user_age,
    u.user_age_at_creation,
    u.user_school_type,
    u.user_is_active,
    u.user_role,
    u.user_is_email_validated,
    u.user_phone_validation_status,
    u.user_has_validated_email,
    u.user_has_enabled_marketing_push,
    u.user_subscribed_themes,
    u.is_theme_subscribed,
    case
        when u.user_role is null and u.user_age in (15, 16)
        then '15_16'
        when u.user_role is null and (u.user_age >= 17 or u.user_age <= 14)
        then 'GENERAL_PUBLIC'
        else u.user_role
    end as user_category,
    -- Current suspension: the account's latest suspension action, when it is a
    -- USER_SUSPENDED (a later USER_UNSUSPENDED lifts it)
    case
        when ah.action_type = 'USER_SUSPENDED' then ah.action_history_reason
    end as user_suspension_reason,
    case
        when ah.action_type = 'USER_SUSPENDED' then date(ah.action_date)
    end as user_suspension_date
from {{ ref("int_applicative__user") }} as u
left join
    {{ ref("int_applicative__action_history") }} as ah
    on u.user_id = ah.user_id
    and ah.action_history_rk = 1
