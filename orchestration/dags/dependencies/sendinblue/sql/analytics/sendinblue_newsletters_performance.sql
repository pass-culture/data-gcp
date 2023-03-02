
-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

WITH sendinblue_newsletter as (
    SELECT 
        *
        , row_number() over( partition by campaign_id order by update_date desc) as rank_update
    FROM `{{ bigquery_raw_dataset }}.sendinblue_newsletters_histo`
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
    campaign_id
    , campaign_utm
    , campaign_name
    , campaign_sent_date
    , share_link
    , audience_size
    , open_number
    , unsubscriptions
    , user_current_deposit_type
    , session_number
    , offer_consultation_number
    , booking_number
    , favorites_number
    , date(update_date) as update_date

FROM sendinblue_newsletter
LEFT JOIN user_traffic
ON sendinblue_newsletter.campaign_utm = user_traffic.traffic_campaign