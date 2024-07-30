with bookings_grouped_by_stock as (
    select
        booking.stock_id,
        SUM(booking.booking_quantity) as number_of_booking
    from
        {{ source('raw','applicative_database_booking') }} as booking
        left join {{ source('raw','applicative_database_stock') }} as stock
            on booking.stock_id = stock.stock_id
    where
        booking.booking_is_cancelled = False
    group by
        booking.stock_id
)

select
    stock.stock_id,
    case
        when stock.stock_quantity is Null then Null
        else GREATEST(
            stock.stock_quantity - COALESCE(bookings_grouped_by_stock.number_of_booking, 0),
            0
        )
    end as available_stock_information
from
    {{ source('raw','applicative_database_stock') }} as stock
    left join bookings_grouped_by_stock on bookings_grouped_by_stock.stock_id = stock.stock_id
