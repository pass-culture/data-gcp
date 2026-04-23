select id as advice_id, offer_id, venue_id, advice_content, advice_author, updated_at
from {{ source("raw", "applicative_database_pro_advice") }}
