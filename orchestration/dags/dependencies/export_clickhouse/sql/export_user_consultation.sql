SELECT  
    event_date,
    event_timestamp,
    offer_id as offer_id,
    user_id as user_id,
    origin
FROM `{{ bigquery_analytics_prod }}.firebase_events` 
WHERE 
event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL {{ params.days }} DAY) and DATE("{{ ds() }}")
and event_name = "ConsultOffer"
AND offer_id is not null
AND user_id is not null