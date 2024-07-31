select
    offer.*,
    offerer.offerer_id
from {{ source('raw','applicative_database_stock') }} as stock
    join {{ ref('offer') }} as offer
        on stock.offer_id = offer.offer_id
    left join {{ ref('available_stock_information') }} as av_stock
        on stock.stock_id = av_stock.stock_id
    left join {{ source('raw','applicative_database_venue') }} as venue
        on offer.venue_id = venue.venue_id
    left join {{ source('raw','applicative_database_offerer') }} as offerer
        on venue.venue_managing_offerer_id = offerer.offerer_id
where (
    DATE(stock.stock_booking_limit_date) > CURRENT_DATE
    or stock.stock_booking_limit_date is NULL
)
and (
    DATE(stock.stock_beginning_date) > CURRENT_DATE
    or stock.stock_beginning_date is NULL
)
and offer.offer_is_active
and (
    available_stock_information > 0
    or available_stock_information is NULL
)
and not stock_is_soft_deleted
and offer_validation = 'APPROVED'
