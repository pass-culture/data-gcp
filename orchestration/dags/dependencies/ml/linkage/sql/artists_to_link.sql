WITH
  authors_table AS (
  SELECT
    author AS artist_name,
    offer_category_id,
    is_synchronised,
    COUNT(booking_cnt) AS offer_number,
    SUM(IFNULL(booking_cnt, 0)) AS total_booking_count,
    'author' AS artist_type
  FROM
    `{{ bigquery_analytics_dataset }}`.enriched_offer_data
  WHERE
    offer_category_id IN ("CINEMA",
      "MUSIQUE_LIVE",
      "SPECTACLE",
      "MUSIQUE_ENREGISTREE",
      "LIVRE")
    AND author IS NOT NULL
    AND author != ""
  GROUP BY
    author,
    offer_category_id,
    is_synchronised ),
  performers_table AS (
  SELECT
    performer AS artist_name,
    offer_category_id,
    is_synchronised,
    COUNT(booking_cnt) AS offer_number,
    SUM(IFNULL(booking_cnt, 0)) AS total_booking_count,
    'performer' AS artist_type
  FROM
    `{{ bigquery_analytics_dataset }}`.enriched_offer_data
  WHERE
    offer_category_id IN ( "MUSIQUE_LIVE",
      "SPECTACLE",
      "MUSIQUE_ENREGISTREE")
    AND performer IS NOT NULL
    AND performer != ""
  GROUP BY
    performer,
    offer_category_id,
    is_synchronised )
SELECT
  *
FROM
  authors_table
UNION ALL
SELECT
  *
FROM
  performers_table