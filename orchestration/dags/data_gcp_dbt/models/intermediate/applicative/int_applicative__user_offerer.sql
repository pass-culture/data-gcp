select
    uo.offererid as offerer_id,
    uo.user_offerer_validation_status,
    u.user_id,
    row_number() over (
        partition by uo.offererid
        order by coalesce(u.user_creation_date, u.user_creation_date)
    ) as user_affiliation_rank,
    u.user_creation_date,
    u.user_department_code,
    u.user_postal_code,
    u.user_role,
    u.user_address,
    u.user_city,
    u.user_last_connection_date,
    u.user_is_email_validated,
    u.user_is_active,
    u.user_has_seen_pro_tutorials,
    u.user_phone_validation_status,
    u.user_has_validated_email,
    u.user_has_enabled_marketing_push,
    u.user_has_enabled_marketing_email
from {{ source("raw", "applicative_database_user_offerer") }} as uo
left join {{ source("raw", "applicative_database_user") }} as u on uo.userid = u.user_id
