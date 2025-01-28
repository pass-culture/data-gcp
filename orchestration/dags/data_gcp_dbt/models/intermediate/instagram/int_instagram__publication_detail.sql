{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "publication_export_date", "data_type": "date"},
            cluster_by="publication_account_name",
        )
    )
}}

SELECT
    -- Partition and cluster fields
    post_detail.post_id AS publication_id,
    post_detail.caption AS publication_caption,

    -- Primary identifiers
    post_detail.permalink AS publication_link,
    post_detail.media_type AS instagram_publication_media_type,

    -- Core metrics
    campaign_tag.post_name AS publication_tag_post_name,
    campaign_tag.macro_objective AS publication_tag_macro_objective,
    campaign_tag.micro_objective AS publication_tag_micro_objective,
    campaign_tag.offer_category AS publication_tag_offer_category,
    DATE(post_detail.export_date) AS publication_export_date,
    CASE
        WHEN post_detail.account_id = '17841463525422101' THEN 'instagram_bookclubdupass'
        WHEN post_detail.account_id = '17841410129457081' THEN 'instagram_passcultureofficiel'
    END AS publication_account_name,

    -- Content information
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S+0000', post_detail.posted_at)) AS publication_creation_date,
    SAFE_CAST(post_detail.reach AS INT64) AS publication_reach,
    SAFE_CAST(post_detail.video_views AS INT64) AS publication_video_views,
    SAFE_CAST(post_detail.likes AS INT64) AS publication_likes,

    -- Campaign tags
    SAFE_CAST(post_detail.shares AS INT64) AS publication_shares,
    SAFE_CAST(post_detail.comments AS INT64) AS publication_comments,
    SAFE_DIVIDE((post_detail.likes + post_detail.shares + post_detail.comments), post_detail.reach) AS publication_engagement_rate,
    campaign_tag.media_id IS NOT null AS publication_is_tagged,

    -- Instagram specific metrics
    SAFE_CAST(post_detail.saved AS INT64) AS instagram_publication_saved,
    SAFE_CAST(post_detail.total_interactions AS INT64) AS instagram_publication_total_interactions,
    SAFE_CAST(post_detail.impressions AS INT64) AS instagram_publication_impressions,
    SAFE_CAST(post_detail.follows AS INT64) AS instagram_publication_follows,
    SAFE_CAST(post_detail.profile_visits AS INT64) AS instagram_publication_profile_visits,
    SAFE_CAST(post_detail.profile_activity AS INT64) AS instagram_publication_profile_activity

FROM {{ source("raw", "instagram_post_detail") }} AS post_detail
LEFT JOIN {{ source("raw", "gsheet_instagram_campaign_tag") }} AS campaign_tag
    ON post_detail.url_id = campaign_tag.media_id
{% if is_incremental() %}
    WHERE DATE(post_detail.export_date)
    BETWEEN date_sub(date('{{ ds() }}'), interval 1 day) AND date('{{ ds() }}')
{% endif %}
