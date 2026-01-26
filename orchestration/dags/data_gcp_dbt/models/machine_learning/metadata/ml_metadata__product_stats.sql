select
    cast(offer_product_id as int64) as offer_product_id,
    logical_or(offer_is_bookable) as has_bookable_offer,
    array_agg(offer_name order by total_individual_bookings desc limit 1)[
        offset(0)
    ] as offer_name,
    sum(coalesce(total_individual_bookings, 0)) as total_booking_count
from {{ ref("mrt_global__offer") }}
group by offer_product_id
