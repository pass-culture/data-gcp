WITH ranked_offers AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY unique_session_id
      ORDER BY event_timestamp DESC
    ) AS rn
  FROM {{ ref("int_firebase__pro_event") }}
  WHERE offerer_id IS NOT NULL
),

ranked_venues AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY unique_session_id
      ORDER BY event_timestamp DESC
    ) AS rn
  FROM {{ ref("int_firebase__pro_event") }}
  WHERE venue_id IS NOT NULL
)

SELECT
  DISTINCT e.unique_session_id,
  ro.offerer_id,
  rv.venue_id
FROM {{ ref("int_firebase__pro_event") }} e
LEFT JOIN ranked_offers AS ro ON e.unique_session_id = ro.unique_session_id
LEFT JOIN ranked_venues AS rv ON e.unique_session_id = rv.unique_session_id
WHERE (ro.rn = 1 OR ro.rn IS NULL)
AND (rv.rn = 1 OR rv.rn IS NULL)
