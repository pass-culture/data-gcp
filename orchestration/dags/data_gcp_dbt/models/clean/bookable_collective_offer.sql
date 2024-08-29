
select
    collective_offer.collective_offer_id,
    collective_offer.venue_id,
    venue.venue_managing_offerer_id as offerer_id
from {{ source('raw','applicative_database_collective_stock') }} as collective_stock
    join {{ source('raw','applicative_database_collective_offer') }} as collective_offer
        on collective_stock.collective_offer_id = collective_offer.collective_offer_id
    left join {{ source('raw','applicative_database_venue') }} as venue
        on collective_offer.venue_id = venue.venue_id
where collective_offer.collective_offer_is_active
    and (
        DATE(collective_stock.collective_stock_booking_limit_date_time) > CURRENT_DATE
        or collective_stock.collective_stock_booking_limit_date_time is NULL
    )
    and (
        DATE(collective_stock.collective_stock_beginning_date_time) > CURRENT_DATE
        or collective_stock.collective_stock_beginning_date_time is NULL
    )
union all
select
    template.collective_offer_id,
    template.venue_id,
    venue.venue_managing_offerer_id as offerer_id
from
    {{ source('raw','applicative_database_collective_offer_template') }} as template
    join {{ source('raw','applicative_database_venue') }} as venue on venue.venue_id = template.venue_id
