SELECT
    futur_offer_id,
    offer_id,
    offer_publication_date
FROM {{ source('raw', 'applicative_database_futur_offer') }} 