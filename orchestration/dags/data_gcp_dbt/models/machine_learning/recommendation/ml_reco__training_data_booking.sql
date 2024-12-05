select
    booking.user_id,
    cast(user.user_age as int64) as user_age,
    "BOOKING" as event_type,
    booking.booking_creation_date as event_date,
    extract(hour from booking.booking_created_at) as event_hour,
    extract(dayofweek from booking.booking_created_at) as event_day,
    extract(month from booking.booking_created_at) as event_month,
    offer.item_id as item_id,
    offer.offer_subcategory_id as offer_subcategory_id,
    offer.offer_category_id as offer_category_id,
    offer.genres,
    offer.rayon,
    offer.type,
    offer.venue_id,
    offer.venue_name,
from {{ ref("int_global__booking") }} booking
join {{ ref("int_global__offer") }} offer on offer.offer_id = booking.offer_id
inner join {{ ref("int_global__user") }} user on user.user_id = booking.user_id
where booking.booking_creation_date >= date_sub(date("{{ ds() }}"), interval 6 month)
