select id as advice_id, offer_id, venue_id, advice_content, advice_author, update_date
from {{ source("raw", "applicative_database_advice") }}
