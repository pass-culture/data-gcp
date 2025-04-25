select
    stock_id,
    stock_beginning_date,
    stock_last_provider_id,
    stock_booking_limit_date,
    stock_creation_date,
    stock_features,
    stock_price,
    stock_quantity,
    offer_id,
    offerer_id,
    price_category_id as  price_categoryId,
    price_category_label,
    price_category_label_id as  price_category_labelId
from {{ ref("mrt_global__stock") }}
