SELECT 
    *
FROM {{ source('raw', 'applicative_database_venue_bank_account_link') }}