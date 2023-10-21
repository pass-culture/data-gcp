SELECT  
    event_date, 
    event_name,
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_params,
    platform,
    'pro' as origin
FROM `{{ bigquery_clean_dataset }}.clean_prod.firebase_pro_events_{{ yyyymmdd(ds) }}` 
WHERE NOT REGEXP_CONTAINS(event_name, '^[a-z]+(_[a-z]+)*$')
