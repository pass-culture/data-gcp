SELECT
    *
FROM {{ source('raw', 'applicative_database_price_category_label') }}