SELECT
    user_id
    , session_id
    , unique_session_id
    , event_date
    , event_timestamp
    , app_version
    , similar_offer_playlist_type
    , is_algolia_recommend
    , reco_call_id
    ,offer_id
    , similar_offer_id
    ,booking_id
    ,event_name
    , CASE WHEN event_name = 'PlaylistVerticalScroll' THEN 'display' ELSE 'convert' END AS event_type
FROM `{{ bigquery_analytics_dataset }}.firebase_events`
WHERE (event_name = 'PlaylistVerticalScroll'
    OR (event_name = 'ConsultOffer' AND similar_offer_id IS NOT NULL)
    OR (event_name = 'BookingConfirmation' AND similar_offer_id IS NOT NULL)
    )