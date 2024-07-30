SELECT
    id as user_id,
    email,
    date_joined,
    last_login,
    is_superuser
FROM {{ source("raw", "metabase_core_user") }}
