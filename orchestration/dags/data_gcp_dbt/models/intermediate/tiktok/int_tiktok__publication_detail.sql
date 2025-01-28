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
    video_detail.item_id AS publication_id,
    video_detail.reach AS publication_reach,

    -- Primary identifiers
    video_detail.video_views AS publication_video_views,
    video_detail.likes AS publication_likes,

    -- Core metrics
    video_detail.shares AS publication_shares,
    video_detail.comments AS publication_comments,
    video_detail.caption AS publication_caption,
    video_detail.share_url AS publication_link,
    campaign_tag.post_name AS publication_tag_post_name,
    campaign_tag.macro_objective AS publication_tag_macro_objective,

    -- Content information
    campaign_tag.micro_objective AS publication_tag_micro_objective,
    campaign_tag.offer_category AS publication_tag_offer_category,
    video_detail.average_time_watched AS tiktok_publication_average_time_watched,

    -- Campaign tags
    video_detail.video_duration AS tiktok_publication_video_duration,
    video_detail.full_video_watched_rate AS tiktok_publication_full_video_watched_rate,
    DATE(video_detail.export_date) AS publication_export_date,
    CONCAT('tiktok_', video_detail.account) AS publication_account_name,

    -- TikTok specific metrics
    DATE(TIMESTAMP_SECONDS(CAST(video_detail.create_time AS INT64))) AS publication_creation_date,
    SAFE_DIVIDE((video_detail.likes + video_detail.shares + video_detail.comments), video_detail.reach) AS publication_engagement_rate,
    campaign_tag.tiktotk_id IS NOT null AS publication_is_tagged,
    SAFE_CAST(video_detail.total_time_watched AS INT64) AS tiktok_publication_total_time_watched

FROM {{ source("raw", "tiktok_video_detail") }} AS video_detail
LEFT JOIN {{ source("raw", "gsheet_tiktok_campaign_tag") }} AS campaign_tag
    ON video_detail.item_id = campaign_tag.tiktotk_id
{% if is_incremental() %}
    WHERE DATE(video_detail.export_date)
    BETWEEN date_sub(date('{{ ds() }}'), interval 1 day) AND date('{{ ds() }}')
{% endif %}
