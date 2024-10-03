{{ config(materialized="view") }}

select
    resource.labels.namespace_name as environement,
    cast(jsonpayload.user_id as int64) as user_id,
    jsonpayload.message as message,
    coalesce(
        cast(jsonpayload.extra.booking as int64),
        cast(jsonpayload.extra.booking_id as int64)
    ) as booking,
    jsonpayload.extra.reason as reason,
    coalesce(
        cast(jsonpayload.extra.offer as int64),
        cast(jsonpayload.extra.offer_id as int64)
    ) as offer,
    coalesce(
        cast(jsonpayload.extra.venue as int64),
        cast(jsonpayload.extra.venue_id as int64)
    ) as venue,
    coalesce(
        cast(jsonpayload.extra.product as int64),
        cast(jsonpayload.extra.product_id as int64)
    ) as product,
    cast(jsonpayload.extra.stock_id as int64) as stock,
    cast(jsonpayload.extra.old_quantity as int64) as stock_old_quantity,
    cast(jsonpayload.extra.stock_quantity as int64) as stock_new_quantity,
    jsonpayload.extra.old_price as stock_old_price,
    jsonpayload.extra.stock_price as stock_new_price,
    cast(jsonpayload.extra.stock_dnbookedquantity as int64) as stock_booking_quantity,
    jsonpayload.extra.eans as list_of_eans_not_found,
    timestamp
from {{ source("raw", "stdout") }}
where
    date(timestamp) >= date_sub(current_date, interval 90 day)
    and (
        jsonpayload.message = "Booking has been cancelled"
        or jsonpayload.message = "Offer has been created"
        or jsonpayload.message = "Offer has been updated"
        or jsonpayload.message = "Booking was marked as used"
        or jsonpayload.message = "Booking was marked as unused"
        or jsonpayload.message = "Successfully updated stock"
        or jsonpayload.message = "Some provided eans were not found"
    )
