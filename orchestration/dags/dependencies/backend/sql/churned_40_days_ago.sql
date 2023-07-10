SELECT
    CAST("{{ today() }}" AS DATETIME) as execution_date
    ,enriched_venue_data.venue_id
    ,enriched_venue_data.venue_booking_email
FROM analytics_prod.enriched_venue_data
WHERE venue_is_permanent
AND DATE_DIFF(current_date, venue_last_bookable_offer_date, DAY) = 40