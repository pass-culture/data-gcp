with user_with_one_booking as(
SELECT user_id,last_booking_date,first_booking_date,user_postal_code
FROM `{{ bigquery_analytics_dataset }}.enriched_user_data`
where last_booking_date=first_booking_date
and last_booking_date>= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)
select ebd.user_id,uob.user_postal_code,evd.venue_latitude,evd.venue_longitude,ebd.offer_id,eom.subcategory_id,eom.search_group_name
from `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
JOIN user_with_one_booking uob on ebd.user_id=uob.user_id
JOIN `{{ bigquery_clean_dataset }}.offer_metadata` eom on eom.offer_id=ebd.offer_id
JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` evd on evd.venue_id=ebd.venue_id