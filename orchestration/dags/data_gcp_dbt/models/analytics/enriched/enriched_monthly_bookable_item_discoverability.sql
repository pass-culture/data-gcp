{{
    config(
        tags = "monthly",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'month', 'data_type': 'date'},
    )
}}

WITH monthly_bookable_items AS (
SELECT
    DATE_TRUNC(partition_date, MONTH) AS month
    , item_id
    , offer_subcategory_id
    , offer_category_id
    , COUNT(DISTINCT offer_id) AS nb_bookable_offers
    , COUNT(DISTINCT partition_date) AS nb_bookable_days
FROM {{ref('bookable_offer_history')}} bookable_offer_history
   {% if is_incremental() %} -- recalculate latest day's DATA + previous
WHERE
  DATE(partition_date) >= DATE(_dbt_max_partition)
{% endif %}
GROUP BY 1,2,3,4
),

monthly_consulted_items AS (
SELECT
    DATE_TRUNC(event_date, MONTH) AS month
    , item_id
    , SUM(nb_daily_consult) AS nb_monthly_consult
    , SUM(CASE WHEN origin = 'search' THEN nb_daily_consult ELSE NULL END) AS nb_monthly_search_consult
    , SUM(CASE WHEN origin IN ('home', 'video','videoModal', 'highlightOffer', 'thematicHighlight','exclusivity') THEN nb_daily_consult ELSE NULL END) AS nb_monthly_home_consult
    , SUM(CASE WHEN origin = 'venue' THEN nb_daily_consult ELSE NULL END) AS nb_monthly_venue_consult
    , SUM(CASE WHEN origin = 'favorites' THEN nb_daily_consult ELSE NULL END) AS nb_monthly_favorites_consult
    , SUM(CASE WHEN origin IN ('similar_offer','same_artist_playlist') THEN nb_daily_consult ELSE NULL END) AS nb_monthly_similar_offer_consult
    , SUM(CASE WHEN origin NOT IN ('search','home', 'video','videoModal', 'highlightOffer', 'thematicHighlight','exclusivity','venue','favorites','similar_offer','same_artist_playlist') THEN nb_daily_consult ELSE NULL END) AS nb_monthly_other_channel_offer_consult
FROM {{ref('firebase_daily_offer_consultation_data')}} firebase_daily_offer_consultation_data
   {% if is_incremental() %} -- recalculate latest day's DATA + previous
WHERE
  DATE(event_date) >= DATE_SUB(DATE(_dbt_max_partition), INTERVAL 1 MONTH)
{% endif %}
GROUP BY 1,2
)

SELECT *
FROM monthly_bookable_items
LEFT JOIN monthly_consulted_items USING(month, item_id)



