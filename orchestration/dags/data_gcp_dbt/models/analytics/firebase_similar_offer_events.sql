{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date'},
    )
}}

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
    , f_events.offer_id
    , offer_item_ids.item_id
    , similar_offer_id
    , similar_offer_item_ids.item_id AS similar_item_id
    , booking_id
    , event_name
    , f_events.user_location_type
    , CASE WHEN event_name = 'PlaylistVerticalScroll' THEN 'display' ELSE 'convert' END AS event_type
FROM {{ ref('int_firebase__native_event') }} f_events
INNER JOIN {{ ref('offer_item_ids') }} offer_item_ids USING(offer_id)
LEFT JOIN {{ ref('offer_item_ids') }} similar_offer_item_ids ON similar_offer_item_ids.offer_id = f_events.similar_offer_id
WHERE (event_name = 'PlaylistVerticalScroll'
    OR (event_name = 'ConsultOffer' AND similar_offer_id IS NOT NULL)
    OR (event_name = 'BookingConfirmation' AND similar_offer_id IS NOT NULL)
    )
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
    {% endif %}
