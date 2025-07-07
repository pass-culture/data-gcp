{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "publication_export_date", "data_type": "date"},
            cluster_by="publication_account_name",
            on_schema_change="append_new_columns",
        )
    )
}}

select
    video_detail.item_id as publication_id,
    video_detail.reach as publication_reach,
    video_detail.video_views as publication_video_views,
    video_detail.likes as publication_likes,
    video_detail.shares as publication_shares,
    video_detail.comments as publication_comments,
    video_detail.caption as publication_caption,
    video_detail.share_url as publication_link,
    campaign_tag.post_name as publication_tag_post_name,
    campaign_tag.macro_objective as publication_tag_macro_objective,
    campaign_tag.micro_objective as publication_tag_micro_objective,
    campaign_tag.offer_category as publication_tag_offer_category,
    campaign_tag.region as publication_tag_region_name,
    video_detail.average_time_watched as average_tiktok_publication_time_watched,
    video_detail.video_duration as tiktok_publication_video_duration,
    video_detail.full_video_watched_rate as tiktok_publication_full_video_watched_rate,
    date(video_detail.export_date) as publication_export_date,
    concat('tiktok_', video_detail.account) as publication_account_name,
    date(
        timestamp_seconds(cast(video_detail.create_time as int64))
    ) as publication_creation_date,
    safe_divide(
        (video_detail.likes + video_detail.shares + video_detail.comments),
        video_detail.reach
    ) as publication_engagement_rate,
    campaign_tag.tiktotk_id is not null as is_publication_tagged,
    safe_cast(
        video_detail.total_time_watched as int64
    ) as total_tiktok_publication_time_watched
from {{ source("raw", "tiktok_video_detail") }} as video_detail
left join
    {{ source("raw", "gsheet_tiktok_campaign_tag") }} as campaign_tag
    on video_detail.item_id = campaign_tag.tiktotk_id
{% if is_incremental() %}
    where
        date(video_detail.export_date)
        between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
{% endif %}
