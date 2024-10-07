select offer.isbn, count(distinct booking_id) as nb_booking
from {{ ref("mrt_global__offer") }} offer
left join {{ ref("mrt_global__booking") }} booking on offer.offer_id = booking.offer_id
where
    booking.booking_created_at >= date_sub(current_date, interval 30 day)
    and booking.booking_is_cancelled is false
group by offer.isbn
having offer.isbn is not null
order by nb_booking desc
