select *
from {{ source('raw', 'applicative_database_offer_report') }}
