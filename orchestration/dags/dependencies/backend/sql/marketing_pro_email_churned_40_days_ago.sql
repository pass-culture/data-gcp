SELECT
    CAST("{{ today() }}" AS DATETIME) as execution_date
    ,venue_id
    ,venue_booking_email
FROM `{{ bigquery_analytics_dataset }}.global_venue`
WHERE venue_is_permanent
AND venue_booking_email IS NOT NULL
AND DATE_DIFF(current_date, venue_last_bookable_offer_date, DAY) = 40
