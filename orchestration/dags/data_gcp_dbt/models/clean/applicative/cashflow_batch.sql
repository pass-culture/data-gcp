SELECT
    *
FROM {{ source('raw', 'applicative_database_cashflow_batch') }}