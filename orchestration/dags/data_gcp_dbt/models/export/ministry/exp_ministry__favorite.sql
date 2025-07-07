select favorite_id, favorite_created_at, user_id, offer_id
from {{ ref("mrt_global__favorite") }}
