SELECT
    *
FROM {{ source('raw', 'applicative_database_cds_cinema_details') }}