
-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

WITH sendinblue_transactional as (
    SELECT DISTINCT 
        *
    FROM `{{ bigquery_clean_dataset }}.sendinblue_transactional`
),

user_traffic as (
    SELECT
        traffic_campaign
        , user_current_deposit_type
        , count(distinct session_id) as session_number
        , count(distinct case when event_name = 'ConsultOffer' then offer_id else null end) as offer_consultation_number
        , count(distinct case when event_name = 'BookingConfirmation' then booking_id else null end) as booking_number
        , count(distinct case when event_name = 'HasAddedOfferToFavorites' then offer_id else null end) as favorites_number
    FROM `{{ bigquery_int_firebase_dataset }}.native_event` firebase
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` user
    ON firebase.user_id = user.user_id
    WHERE traffic_campaign is not null
    AND lower(traffic_medium) like "%email%"
    AND event_name in ('ConsultOffer', 'BookingConfirmation', 'HasAddedOfferToFavorites')
    AND event_date >= DATE_SUB(DATE("{{ ds }}"),  INTERVAL 30 DAY)
    GROUP BY 1, 2
)

SELECT 
    template as template_id
    , tag
    , delivered_count as audience_size
    , unique_opened_count as open_number
    , unsubscribed_count as unsubscriptions
    , user_current_deposit_type
    , session_number
    , offer_consultation_number
    , booking_number
    , favorites_number
    , date(update_date) as update_date

FROM sendinblue_transactional
LEFT JOIN user_traffic
ON sendinblue_transactional.tag = user_traffic.traffic_campaign