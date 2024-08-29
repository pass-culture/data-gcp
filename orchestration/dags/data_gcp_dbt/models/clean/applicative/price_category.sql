select *
from {{ source('raw', 'applicative_database_price_category') }}
