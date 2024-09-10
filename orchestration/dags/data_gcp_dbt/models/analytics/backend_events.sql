{{
  config(materialized='view')
}}

select
    resource.labels.namespace_name as environement,
    CAST(jsonpayload.user_id as INT64) as user_id,
    jsonpayload.message as message,
    COALESCE(
        CAST(jsonpayload.extra.booking as INT64),
        CAST(jsonpayload.extra.booking_id as INT64)
    ) as booking,
    jsonpayload.extra.reason as reason,
    COALESCE(
        CAST(jsonpayload.extra.offer as INT64),
        CAST(jsonpayload.extra.offer_id as INT64)
    ) as offer,
    COALESCE(
        CAST(jsonpayload.extra.venue as INT64),
        CAST(jsonpayload.extra.venue_id as INT64)
    ) as venue,
    COALESCE(
        CAST(jsonpayload.extra.product as INT64),
        CAST(jsonpayload.extra.product_id as INT64)
    ) as product,
    CAST(jsonpayload.extra.stock_id as INT64) as stock,
    CAST(jsonpayload.extra.old_quantity as INT64) as stock_old_quantity,
    CAST(jsonpayload.extra.stock_quantity as INT64) as stock_new_quantity,
    jsonpayload.extra.old_price as stock_old_price,
    jsonpayload.extra.stock_price as stock_new_price,
    CAST(jsonpayload.extra.stock_dnbookedquantity as INT64) as stock_booking_quantity,
    jsonpayload.extra.eans as list_of_eans_not_found,
    timestamp
from {{ source("raw","stdout") }}
where
    DATE(timestamp) >= DATE_SUB(CURRENT_DATE, interval 90 day)
    and
    (
        jsonpayload.message = "Booking has been cancelled"
        or jsonpayload.message = "Offer has been created"
        or jsonpayload.message = "Offer has been updated"
        or jsonpayload.message = "Booking was marked as used"
        or jsonpayload.message = "Booking was marked as unused"
        or jsonpayload.message = "Successfully updated stock"
        or jsonpayload.message = "Some provided eans were not found"
    )
