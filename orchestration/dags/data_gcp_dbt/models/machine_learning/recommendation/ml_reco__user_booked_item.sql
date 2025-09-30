select distinct user_id, item_id
from {{ ref("mrt_global__booking") }}
where
    booking_is_cancelled = false
    and stock_id is not null
    and user_id is not null
    and offer_id is not null
