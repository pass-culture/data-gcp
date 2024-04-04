SELECT
   resource.labels.namespace_name as environement,
   CAST(jsonPayload.user_id as INT64) as user_id,
   jsonPayload.message as message,
   COALESCE(
     CAST(jsonPayload.extra.booking as INT64),
     CAST(jsonPayload.extra.booking_id as INT64)
   ) as booking,
   jsonPayload.extra.reason as reason,
   COALESCE(
     CAST(jsonPayload.extra.offer as INT64), 
     CAST(jsonPayload.extra.offer_id as INT64)
   ) as offer,
   COALESCE(
     CAST(jsonPayload.extra.venue as INT64),
     CAST(jsonPayload.extra.venue_id as INT64) 
   ) as venue,
   COALESCE(
     CAST(jsonPayload.extra.product as INT64),
     CAST(jsonPayload.extra.product_id as INT64) 
   ) as product,
   CAST(jsonPayload.extra.stock_id as INT64) as stock,
   CAST(jsonPayload.extra.old_quantity as INT64) as stock_old_quantity,
   CAST(jsonPayload.extra.stock_quantity as INT64) as stock_new_quantity,
   jsonPayload.extra.old_price as stock_old_price,
   jsonPayload.extra.stock_price as stock_new_price,
   CAST(jsonPayload.extra.stock_dnbookedquantity as INT64) as stock_booking_quantity,
   jsonPayload.extra.eans as list_of_eans_not_found,
   timestamp,
FROM
 `{{ bigquery_raw_dataset }}.stdout`
WHERE 
DATE(timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 90 day)
AND 
(
  jsonPayload.message="Booking has been cancelled"
  OR jsonPayload.message="Offer has been created"
  OR jsonPayload.message="Offer has been updated"
  OR jsonPayload.message="Booking was marked as used"
  OR jsonPayload.message="Booking was marked as unused"
  OR jsonPayload.message="Successfully updated stock"
  OR jsonPayload.message="Some provided eans were not found"
)
