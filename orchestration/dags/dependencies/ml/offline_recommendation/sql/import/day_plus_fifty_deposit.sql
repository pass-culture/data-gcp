-- '50 days post credit' 
with user_50days_after_credit as(
SELECT eud.user_id
FROM `{{ bigquery_analytics_dataset }}.enriched_user_data` eud
where date(user_deposit_creation_date)= DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY)
)
select ebd.user_id,evd.venue_latitude,evd.venue_longitude,ebd.offer_id,eom.subcategory_id,eom.search_group_name
from `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
JOIN user_50days_after_credit uin on uin.user_id=ebd.user_id
JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_metadata` eom on eom.offer_id=ebd.offer_id
JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` evd on evd.venue_id=ebd.venue_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY ebd.user_id ORDER BY ebd.booking_creation_date DESC) = 1
order by user_id DESC