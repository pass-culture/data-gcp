WITH ranked_offers AS (
  SELECT
    unique_session_id,
    event_timestamp,
    offerer_id
  FROM {{ ref("int_firebase__pro_event") }}
  WHERE offerer_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id ORDER BY event_timestamp DESC)=1
),

ranked_venues AS (
  SELECT
    unique_session_id,
    event_timestamp,
    venue_id
  FROM {{ ref("int_firebase__pro_event") }}
  WHERE venue_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id ORDER BY event_timestamp DESC)=1
)

SELECT
  DISTINCT e.unique_session_id,
  e.user_id
  ro.offerer_id,
  rv.venue_id
FROM {{ ref("int_firebase__pro_event") }} e
LEFT JOIN ranked_offers AS ro ON e.unique_session_id = ro.unique_session_id
LEFT JOIN ranked_venues AS rv ON e.unique_session_id = rv.unique_session_id
