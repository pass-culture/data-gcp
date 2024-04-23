with user_with_one_booking as(
SELECT user_id,user_postal_code,first_booking_date as first_booking_date,CURRENT_DATE() as current_date
FROM `{{ bigquery_analytics_dataset }}.enriched_user_data`
where DATE(first_booking_date)=DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
)
select 
ebd.user_id
,uob.user_postal_code
,evd.venue_latitude
,evd.venue_longitude
,uob.first_booking_date
,ebd.booking_creation_date
,uob.current_date
,ebd.offer_id
,eom.subcategory_id
,eom.search_group_name
from `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
JOIN user_with_one_booking uob on ebd.user_id=uob.user_id
JOIN `{{ bigquery_clean_dataset }}.offer_metadata` eom on eom.offer_id=ebd.offer_id
JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` evd on evd.venue_id=ebd.venue_id
where uob.first_booking_date=ebd.booking_creation_date