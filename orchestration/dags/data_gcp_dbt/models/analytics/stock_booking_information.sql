select
    stock.stock_id,
    COALESCE(SUM(booking.booking_quantity), 0) as booking_quantity,
    COALESCE(SUM(case when booking.booking_status = 'CANCELLED' then booking.booking_quantity else NULL end), 0) as booking_cancelled,
    COALESCE(SUM(case when booking.booking_status != 'CANCELLED' then booking.booking_quantity else NULL end), 0) as booking_non_cancelled,
    COALESCE(SUM(case when booking.booking_status = 'REIMBURSED' then booking.booking_quantity else NULL end), 0) as bookings_paid
from {{ ref('stock') }} as stock
    left join {{ ref('booking') }} as booking on booking.stock_id = stock.stock_id
group by
    stock.stock_id
