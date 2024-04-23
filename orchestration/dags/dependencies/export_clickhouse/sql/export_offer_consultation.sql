SELECT  

    event_date,
    offerer_id,
    offer_id,
    venue_id,
    offerer_name,
    venue_name,
    offer_name,
    origin,
    user_role,
    user_age,
    sum(cnt_events) as cnt_events,
FROM `{{ bigquery_analytics_dataset }}.aggregated_daily_offer_consultation_data` 
WHERE event_date BETWEEN date_sub(DATE("{{ ds }}"), INTERVAL {{ params.days }} DAY) and DATE("{{ ds }}")
and event_name = "ConsultOffer"
AND offer_id is not null 
AND offerer_id is not null
GROUP BY 
    event_date,
    offerer_id,
    offer_id,
    venue_id,
    offerer_name,
    venue_name,
    offer_name,
    origin,
    user_role,
    user_age