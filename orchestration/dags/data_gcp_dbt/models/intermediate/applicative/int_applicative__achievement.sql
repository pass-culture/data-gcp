select
    id as achievement_id,
    user_id,
    booking_id,
    name as achievement_name,
    date(unlocked_date) as achievement_unlocked_date,
    date(seen_date) as achievement_seen_date
from {{ ref("raw_applicative__achievement") }}
