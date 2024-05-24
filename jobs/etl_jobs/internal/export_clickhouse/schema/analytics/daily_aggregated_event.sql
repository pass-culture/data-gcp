CREATE VIEW analytics.daily_aggregated_event ON cluster default
AS
SELECT
    toDate(partition_date) AS event_date,
    origin,
    offer_id,
    venue_id,
    sum(is_consult_offer) AS nbr_offer_consultation,
    sum(is_consult_venue) AS nbr_venue_consultation,
    sum(is_add_to_favorites) AS nbr_favorite
FROM intermediate.native_event
WHERE event_name = 'ConsultOffer'
GROUP BY
    event_date,
    origin,
    offer_id,
    venue_id