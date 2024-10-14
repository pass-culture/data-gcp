select
    id as favorite_id,
    date(datecreated) as favorite_creation_date,
    datecreated as favorite_created_at,
    userid as user_id,
    offerid as offer_id
from {{ source("raw", "applicative_database_favorite") }}
