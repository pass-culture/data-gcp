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
   timestamp,
FROM
 `{{ bigquery_raw_dataset }}.stdout`
WHERE jsonPayload.message="Booking has been cancelled"
OR jsonPayload.message="Offer has been created"
OR jsonPayload.message="Offer has been updated"
OR jsonPayload.message="Booking was marked as used"
OR jsonPayload.message="Booking was marked as unused"
