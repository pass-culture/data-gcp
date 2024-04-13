SELECT 
    *
FROM {{ source('raw', 'applicative_database_user_pro_new_nav_state') }}