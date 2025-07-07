select
    achievement_id,
    user_id,
    booking_id,
    achievement_name,
    achievement_unlocked_date,
    achievement_seen_date
from {{ ref("int_applicative__achievement") }}
