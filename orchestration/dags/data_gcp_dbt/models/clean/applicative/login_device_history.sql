SELECT
    *
FROM {{ source('raw', 'applicative_database_login_device_history') }}