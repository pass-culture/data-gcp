
-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

WITH sendinblue_transactional as (
    SELECT 
        *
        , row_number() over( partition by template, tag order by update_date desc) as rank_update
    FROM `{{ bigquery_raw_dataset }}.sendinblue_transactional_histo`
    QUALIFY rank_update = 1
),

user_traffic as (
    SELECT
        traffic_campaign
        , user_current_deposit_type
        , count(distinct session_id) as session_number
        , count(distinct case when event_name = 'ConsultOffer' then offer_id else null end) as offer_consultation_number
        , count(distinct case when event_name = 'BookingConfirmation' then booking_id else null end) as booking_number
        , count(distinct case when event_name = 'HasAddedOfferToFavorites' then offer_id else null end) as favorites_number
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` firebase
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` user
    ON firebase.user_id = user.user_id
    WHERE traffic_campaign is not null
    GROUP BY 1, 2
)

SELECT 
    template as template_id
    , tag
    , last_date_delivered
    , count_delivered as audience_size
    , unique_opened as open_number
    , unique_unsubscribed as unsubscriptions
    , user_current_deposit_type
    , session_number
    , offer_consultation_number
    , booking_number
    , favorites_number
    , date(update_date) as update_date

FROM sendinblue_transactional
LEFT JOIN user_traffic
ON sendinblue_transactional.tag = user_traffic.traffic_campaign