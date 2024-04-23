with user_qpi as (
  SELECT
    user_id,
    STRING_AGG(
      DISTINCT subcategories
      order by
        subcategories
    ) as qpi_subcategory_ids
  FROM
    `{{ bigquery_analytics_dataset }}`.enriched_qpi_answers
  where subcategories <> 'none'
  group by
    user_id
)
SELECT
  firebase_agg.user_id,
  firebase_agg.consult_offer,
  stats_reco.booking_cnt,
  ROUND(stats_reco.user_theoretical_remaining_credit, 0) AS user_theoretical_remaining_credit,
  stats_reco.has_added_offer_to_favorites,
  STRING_AGG(DISTINCT firebase.query, " ") AS user_queries,
  uqpi.qpi_subcategory_ids AS qpi_subcategory_ids

FROM
  `{{ bigquery_analytics_dataset }}`.firebase_aggregated_users firebase_agg
  JOIN `{{ bigquery_ml_reco_dataset }}`.user_statistics stats_reco ON stats_reco.user_id = firebase_agg.user_id
  JOIN `{{ bigquery_analytics_dataset }}`.firebase_events firebase ON firebase.user_id = firebase_agg.user_id
  LEFT JOIN user_qpi uqpi on uqpi.user_id=firebase_agg.user_id
GROUP BY
  firebase_agg.user_id,
  firebase_agg.consult_offer,
  stats_reco.booking_cnt,
  stats_reco.user_theoretical_remaining_credit,
  stats_reco.has_added_offer_to_favorites,
  uqpi.qpi_subcategory_ids