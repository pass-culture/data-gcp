CREATE VIEW analytics.daily_aggregated_offer_consultation ON cluster default
AS
SELECT
    toDate(partition_date) AS event_date,
    origin,
    offer_id,
    sum(cnt_events) AS sumCountViews
FROM intermediate.offer_consultation
GROUP BY
    event_date,
    origin,
    offer_id