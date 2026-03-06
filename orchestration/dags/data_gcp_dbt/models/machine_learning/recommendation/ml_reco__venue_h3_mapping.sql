{{ config(
    materialized='table'
) }}

{% call set_sql_header(config) %}
CREATE TEMP FUNCTION H3_FROM_LATLNG(lat FLOAT64, lng FLOAT64, res INT64)
RETURNS STRING
LANGUAGE js
OPTIONS (library=["gs://data-team-sandbox-stg/lmontier/h3-js/h3-js.umd.js"])
AS """
  if (lat === null || lng === null) return null;
  return h3.latLngToCell(lat, lng, res);
""";
{% endcall %}

SELECT DISTINCT
    venue_id,
    venue_latitude AS latitude,
    venue_longitude AS longitude,
    H3_FROM_LATLNG(venue_latitude, venue_longitude, 3) AS h3_res3,
    H3_FROM_LATLNG(venue_latitude, venue_longitude, 4) AS h3_res4,
    H3_FROM_LATLNG(venue_latitude, venue_longitude, 5) AS h3_res5,
    H3_FROM_LATLNG(venue_latitude, venue_longitude, 6) AS h3_res6
FROM {{ ref('ml_reco__recommendable_offer') }}
WHERE venue_id IS NOT NULL
  AND venue_latitude IS NOT NULL
  AND venue_longitude IS NOT NULL