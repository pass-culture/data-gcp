SELECT  
    event_date,
    event_timestamp,
    offer_id as offer_id,
    user_id as user_id,
    origin
FROM `{{ bigquery_int_firebase_dataset }}.native_event`
WHERE 
event_date BETWEEN date_sub(DATE("{{ ds }}"), INTERVAL {{ params.days }} DAY) and DATE("{{ ds }}")
and event_name = "ConsultOffer"
AND offer_id is not null
AND user_id is not null