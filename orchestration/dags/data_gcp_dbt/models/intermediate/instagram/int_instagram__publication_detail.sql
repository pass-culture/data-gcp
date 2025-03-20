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
    post_detail.post_id as publication_id,
    post_detail.caption as publication_caption,
    post_detail.permalink as publication_link,
    post_detail.media_type as instagram_publication_media_type,
    campaign_tag.post_name as publication_tag_post_name,
    campaign_tag.macro_objective as publication_tag_macro_objective,
    campaign_tag.micro_objective as publication_tag_micro_objective,
    campaign_tag.offer_category as publication_tag_offer_category,
    campaign_tag.region as publication_tag_region_name,
    date(post_detail.export_date) as publication_export_date,
    case
        when post_detail.account_id = '17841463525422101'
        then 'instagram_bookclubdupass'
        when post_detail.account_id = '17841410129457081'
        then 'instagram_passcultureofficiel'
    end as publication_account_name,
    date(
        parse_timestamp('%Y-%m-%dT%H:%M:%S+0000', post_detail.posted_at)
    ) as publication_creation_date,
    safe_cast(post_detail.reach as int64) as publication_reach,
    safe_cast(post_detail.video_views as int64) as publication_video_views,
    safe_cast(post_detail.likes as int64) as publication_likes,
    safe_cast(post_detail.shares as int64) as publication_shares,
    safe_cast(post_detail.comments as int64) as publication_comments,
    safe_divide(
        (post_detail.likes + post_detail.shares + post_detail.comments),
        post_detail.reach
    ) as publication_engagement_rate,
    campaign_tag.media_id is not null as is_publication_tagged,
    safe_cast(post_detail.saved as int64) as instagram_publication_saved,
    safe_cast(
        post_detail.total_interactions as int64
    ) as total_instagram_publication_interactions,
    safe_cast(post_detail.impressions as int64) as instagram_publication_impressions,
    safe_cast(post_detail.follows as int64) as instagram_publication_follows,
    safe_cast(
        post_detail.profile_visits as int64
    ) as instagram_publication_profile_visits,
    safe_cast(
        post_detail.profile_activity as int64
    ) as instagram_publication_profile_activity
from {{ source("raw", "instagram_post_detail") }} as post_detail
left join
    {{ source("raw", "gsheet_instagram_campaign_tag") }} as campaign_tag
    on post_detail.url_id = campaign_tag.media_id
{% if is_incremental() %}
    where
        date(post_detail.export_date)
        between date_sub(date('{{ ds() }}'), interval 1 day) and date('{{ ds() }}')
{% endif %}
