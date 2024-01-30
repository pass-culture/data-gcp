SELECT
    *
FROM {{ source('raw', 'applicative_database_finance_event') }}