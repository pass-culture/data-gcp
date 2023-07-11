SELECT
    CAST("{{ today() }}" AS DATETIME) as execution_date
    ,venue_id
    ,venue_booking_email
FROM `{{ bigquery_analytics_dataset }}.enriched_venue_data`
WHERE venue_is_permanent
AND DATE_DIFF(current_date, venue_last_bookable_offer_date, DAY) = 40