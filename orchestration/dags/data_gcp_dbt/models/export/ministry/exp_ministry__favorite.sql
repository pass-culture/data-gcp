

select favorite_id, favorite_creation_date, user_id, offer_id
from {{ ref("mrt_global__favorite") }}
