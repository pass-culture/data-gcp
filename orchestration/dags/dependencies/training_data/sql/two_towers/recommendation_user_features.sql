SELECT
  firebase_agg.user_id,
  firebase_agg.user_engagement,
  firebase_agg.consult_offer,
  firebase_agg.click_book_offer,
  stats_reco.booking_cnt,
  ROUND(stats_reco.user_theoretical_remaining_credit, 0) AS user_theoretical_remaining_credit,
  stats_reco.has_added_offer_to_favorites,
  STRING_AGG(DISTINCT firebase.query, " ") AS user_queries

FROM
  `{{ bigquery_analytics_dataset }}`.firebase_aggregated_users firebase_agg
  JOIN `{{ bigquery_analytics_dataset }}`.aggregated_user_stats_reco stats_reco ON stats_reco.user_id = firebase_agg.user_id
  JOIN `{{ bigquery_analytics_dataset }}`.firebase_events firebase ON firebase.user_id = firebase_agg.user_id

GROUP BY
  firebase_agg.user_id,
  firebase_agg.user_engagement,
  firebase_agg.consult_offer,
  firebase_agg.click_book_offer,
  stats_reco.booking_cnt,
  stats_reco.user_theoretical_remaining_credit,
  stats_reco.has_added_offer_to_favorites
