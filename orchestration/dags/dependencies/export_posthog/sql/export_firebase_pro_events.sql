SELECT  
    event_date, 
    event_name,
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_params,
    platform,
    app_info.version as app_version,
    'pro' as origin
FROM `{{ bigquery_clean_dataset }}.firebase_pro_events_{{ yyyymmdd(add_days(ds, params.days)) }}` 
WHERE NOT REGEXP_CONTAINS(event_name, '^[a-z]+(_[a-z]+)*$')
