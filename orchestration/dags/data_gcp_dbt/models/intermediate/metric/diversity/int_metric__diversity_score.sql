select
    booking_id,
    booking_creation_date,
    booking_created_at,
    user_id,
    sum(diversity_score) as diversity_score
from {{ ref("int_metric__diversity_daily_booking") }}
group by booking_id, booking_creation_date, booking_created_at, user_id
