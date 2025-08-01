SELECT
    go.item_id,
    go.offer_name,
    go.offer_description,
    SUM(CASE WHEN ne.event_name = 'ConsultOffer' THEN 1 ELSE 0 END) AS item_vues
FROM
    `analytics_prod.native_event` AS ne
JOIN
    `analytics_prod.global_offer` AS go
ON
    ne.offer_id = go.offer_id
WHERE
    ne.event_date >= '2025-07-01'
    AND ne.offer_id IS NOT NULL

GROUP BY
    go.item_id,
    go.offer_name,
    go.offer_description
ORDER BY
    item_vues DESC
LIMIT 50;
