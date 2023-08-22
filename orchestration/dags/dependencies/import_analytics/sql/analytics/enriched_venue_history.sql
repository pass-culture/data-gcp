SELECT
    ,active_date
    ,venue_id
    ,venue_booking_email
    ,venue_siret
    ,venue_is_permanent
    ,venue_type_label
    ,venue_label
    ,banner_url
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM `{{ bigquery_analytics_dataset }}`.`enriched_venue_data`