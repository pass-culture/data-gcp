WITH display_data AS ( -- Séparer les données de display et de conversion
SELECT *
FROM `{{ bigquery_analytics_dataset }}.firebase_similar_offer_events`
WHERE event_type = 'display'
AND user_id IS NOT NULL
AND session_id IS NOT NULL
),

convert_data AS (
SELECT *
FROM `{{ bigquery_analytics_dataset }}.firebase_similar_offer_events`
WHERE event_type = 'convert'
AND user_id IS NOT NULL
AND session_id IS NOT NULL
),

display_and_convert AS (
SELECT
    display_data.user_id
    , display_data.session_id
    , display_data.unique_session_id
    , display_data.event_timestamp
    , display_data.event_date
    , enriched_user_data.user_current_deposit_type
    , display_data.app_version
    , display_data.similar_offer_playlist_type
    , display_data.is_algolia_recommend
    , display_data.reco_call_id
    ,display_data.offer_id
    , display_data.similar_offer_id
    , COUNT(DISTINCT CASE WHEN convert_data.event_name = 'ConsultOffer' THEN convert_data.offer_id ELSE NULL END) AS nb_offers_consulted
    , COUNT(DISTINCT CASE WHEN convert_data.event_name = 'BookingConfirmation' THEN convert_data.offer_id ELSE NULL END) AS nb_offers_booked
    , SUM(CASE WHEN convert_data.event_name = 'BookingConfirmation' THEN delta_diversification ELSE NULL END) AS diversification_score
FROM display_data
LEFT JOIN convert_data ON display_data.unique_session_id = convert_data.unique_session_id
                        AND display_data.offer_id = convert_data.similar_offer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.diversification_booking` AS diversification_booking ON diversification_booking.booking_id = convert_data.booking_id
JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` AS enriched_user_data ON enriched_user_data.user_id = display_data.user_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

data_and_lags AS ( -- Déterminer si un utilisateur a consulté une offre X, vu l'algo d'offres similaires A, consulté depuis A une offre Y, vu l'algo B et consulté depuis B une offre Z
SELECT
    *
    , LAG(similar_offer_id) OVER(PARTITION BY user_id, session_id ORDER BY event_timestamp DESC) AS lag_1
    , LAG(similar_offer_id,2) OVER(PARTITION BY user_id, session_id ORDER BY event_timestamp DESC) AS lag_2
    , LAG(similar_offer_id,3) OVER(PARTITION BY user_id, session_id ORDER BY event_timestamp DESC) AS lag_3
FROM display_and_convert
ORDER BY 1,2,3
)

SELECT DISTINCT
    data_and_lags.* EXCEPT (lag_1,lag_2,lag_3, event_timestamp)
    , CASE WHEN offer_id = lag_1
            OR offer_id = lag_2
            OR offer_id = lag_3
            THEN TRUE ELSE FALSE END AS looped_to_other_offer
FROM data_and_lags