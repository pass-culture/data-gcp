SELECT
    *
FROM {{ source('raw', 'applicative_database_accessibility_provider') }}