SELECT
    *
FROM {{ source('raw', 'applicative_database_payment_message') }}