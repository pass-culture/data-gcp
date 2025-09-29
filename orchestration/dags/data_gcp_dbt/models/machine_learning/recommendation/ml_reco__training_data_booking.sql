-- noqa: disable=all
select
    booking.user_id,
    cast(user.user_age as int64) as user_age,
    "BOOKING" as event_type,
    booking.booking_creation_date as event_date,
    offer.item_id,
    offer.offer_subcategory_id,
    offer.offer_category_id,
    offer.genres,
    offer.rayon,
    offer.type,
    offer.venue_id,
    offer.venue_name,
    extract(hour from booking.booking_created_at) as event_hour,
    extract(dayofweek from booking.booking_created_at) as event_day,
    extract(month from booking.booking_created_at) as event_month
from {{ ref("int_global__booking") }} as booking
inner join {{ ref("int_global__offer") }} as offer on booking.offer_id = offer.offer_id
inner join
    {{ ref("int_global__user_beneficiary") }} as user on booking.user_id = user.user_id
where booking.booking_creation_date >= date_sub(date("{{ ds() }}"), interval 6 month)
