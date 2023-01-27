-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

WITH sendinblue_newsletter as (
    SELECT 
        *
        , row_number() over( partition by campaign_id, domain order by update_date desc) as rank_update
    FROM `{{ bigquery_raw_dataset }}.sendinblue_newsletters_histo`
    QUALIFY rank_update = 1
)


SELECT 
    campaign_id
    , campaign_utm
    , campaign_name
    , campaign_sent_date
    , share_link
    , domain
    , sum(audience_size) as audience_size
    , sum(open_number) as open_number
    , sum(unsubscriptions) as unsubscriptions
    , date(update_date) as update_date
    , count(distinct session_id) as session_number

FROM sendinblue_newsletter
LEFT JOIN `{{ bigquery_analytics_dataset }}.firebase_events` firebase
ON sendinblue_newsletter.campaign_utm = firebase.traffic_campaign

WHERE traffic_campaign IS NOT NULL
GROUP BY 
    campaign_id
    , campaign_utm
    , campaign_name
    , campaign_sent_date
    , share_link
    , domain
    , update_date