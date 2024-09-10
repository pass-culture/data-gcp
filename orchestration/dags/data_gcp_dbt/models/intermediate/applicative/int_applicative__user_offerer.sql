select
    uo.offererid as offerer_id,
    uo.user_offerer_validation_status,
    u.user_id,
    ROW_NUMBER() over (partition by uo.offererid order by COALESCE(u.user_creation_date, u.user_creation_date)) as user_affiliation_rank,
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
    u.user_has_enabled_marketing_email,
    new_nav.eligibility_timestamp,
    new_nav.new_nav_timestamp,
    CASE WHEN new_nav.new_nav_timestamp is not null THEN "Nouvelle interface" ELSE "Ancienne interface" END as nav_type
from {{ source("raw", "applicative_database_user_offerer") }} as uo
    left join {{ source("raw", "applicative_database_user") }} as u on uo.userid = u.user_id
    left join {{ source("raw", "applicative_database_user_pro_new_nav_state") }} as new_nave ON uo.user_id=new_nave.user_id
