with offer_booking_information_view as (
    select
        offer.offer_id,
        COUNT(distinct booking.booking_id) as count_booking
    from
        {{ ref('offer') }} as offer
        left join {{ source('raw', 'applicative_database_stock') }} as stock on stock.offer_id = offer.offer_id
        left join {{ ref('booking') }} as booking on stock.stock_id = booking.stock_id
    where booking_is_used
    group by
        offer_id
),

enriched_items as (
    select
        offer.*,
        offer_ids.item_id,
        IF(offer_type_label is not null, count_booking, null) as count_booking
    from {{ ref('offer_metadata') }} offer
        inner join {{ ref('offer_item_ids') }} offer_ids on offer.offer_id = offer_ids.offer_id
        left join offer_booking_information_view obi on obi.offer_id = offer.offer_id
)

select * except (count_booking, offer_id)
from enriched_items
where item_id is not null
qualify ROW_NUMBER() over (partition by item_id order by count_booking desc) = 1
