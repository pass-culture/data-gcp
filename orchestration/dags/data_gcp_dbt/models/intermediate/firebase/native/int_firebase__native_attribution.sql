WITH campaigns AS (
  SELECT
    user_pseudo_id,
    traffic_campaign,
    traffic_medium,
    traffic_source,
    MIN(event_timestamp) AS campaign_begins_at,
    TIMESTAMP_ADD(MIN(event_timestamp), INTERVAL 1 DAY) AS original_campaign_ends_at
FROM {{ ref("int_firebase__native_event_flattened") }}
  WHERE traffic_campaign IS NOT NULL AND traffic_source IS NOT NULL
  GROUP BY
    user_pseudo_id,
    traffic_campaign,
    traffic_medium,
    traffic_source
),
campaigns_with_next_begin AS (
  SELECT
    *,
    LEAD(campaign_begins_at) OVER (PARTITION BY user_pseudo_id ORDER BY campaign_begins_at) AS next_campaign_begins_at
  FROM
    campaigns
)
SELECT
  user_pseudo_id,
  traffic_campaign,
  traffic_medium,
  traffic_source,
  campaign_begins_at,
  IF(
    next_campaign_begins_at IS NOT NULL AND next_campaign_begins_at < original_campaign_ends_at,
    next_campaign_begins_at,
    original_campaign_ends_at
  ) AS campaign_ends_at
FROM
  campaigns_with_next_begin
