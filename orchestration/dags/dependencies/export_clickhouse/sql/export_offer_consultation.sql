SELECT  
    event_date,
    offer_id as offer_id,
    item_id as item_id,
    origin,
    nb_daily_consult as nb_consultation
FROM `{{ bigquery_analytics_prod }}.firebase_daily_offer_consultation_data` 
WHERE 
event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL params.days DAY) and DATE("{{ ds() }}")
AND offer_id is not null