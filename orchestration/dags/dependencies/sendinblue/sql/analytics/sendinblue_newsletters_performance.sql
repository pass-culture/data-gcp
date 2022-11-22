-- Join with firebase_events to get the number of sessions
-- & compute indicators.
-- *** Missing utm 

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

FROM `{{ bigquery_raw_dataset }}.sendinblue_newsletters` sendinblue
LEFT JOIN `{{ bigquery_analytics_dataset }}.firebase_events` firebase
ON sendinblue.campaign_utm = firebase.traffic_campaign

WHERE traffic_campaign IS NOT NULL
GROUP BY 
    campaign_id
    , campaign_utm
    , campaign_name
    , campaign_sent_date
    , share_link
    , domain