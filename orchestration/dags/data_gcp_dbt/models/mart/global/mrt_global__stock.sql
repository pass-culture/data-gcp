select
    stock_id,
    stock_beginning_date,
    stock_last_provider_id,
    stock_booking_limit_date,
    stock_creation_date,
    stock_features,
    stock_price,
    stock_quantity,
    total_available_stock,
    total_cancelled_bookings,
    total_non_cancelled_bookings,
    total_paid_bookings,
    total_bookings,
    offer_id,
    offer_name,
    offer_subcategory_id,
    offerer_id,
    partner_id,
    price_category_id,
    price_category_label,
    price_category_label_id,
    venue_department_code
from {{ ref("int_global__stock") }}
