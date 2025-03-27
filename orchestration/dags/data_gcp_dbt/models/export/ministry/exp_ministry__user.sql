select
    user_id,
    user_activity,
    user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_birth_date,
    first_deposit_creation_date as user_first_deposit_creation_date,
    first_deposit_type as user_first_deposit_type,
    current_deposit_type as user_current_deposit_type,
    user_creation_date as user_created_at,
    user_activation_date as user_activated_at,
    user_suspension_reason,
    user_is_current_beneficiary,
    user_seniority
from {{ ref("mrt_global__user") }}
