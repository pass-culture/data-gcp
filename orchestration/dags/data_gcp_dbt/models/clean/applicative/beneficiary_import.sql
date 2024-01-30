SELECT
    *
FROM {{ source('raw', 'applicative_database_beneficiary_import') }}