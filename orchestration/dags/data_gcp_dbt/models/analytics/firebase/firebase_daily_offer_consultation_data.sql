SELECT
  event_date,
  e.offer_id,
  offer_item_ids.item_id,
  origin,
  COUNT(*) AS nb_daily_consult
FROM
  {{ SOURCE('analytics',
    'firebase_events') }} e
LEFT JOIN
  {{ref('offer_item_ids')}} offer_item_ids
ON
  offer_item_ids.offer_id = e.offer_id
WHERE
  event_name = 'ConsultOffer' {%
IF
  is_incremental() %} -- recalculate latest day's DATA + previous
WHERE
  DATE(event_date) >= DATE_SUB(DATE(_dbt_max_partition), INTERVAL 1 day) {% endif %}
GROUP BY
  1,
  2,
  3,
  4