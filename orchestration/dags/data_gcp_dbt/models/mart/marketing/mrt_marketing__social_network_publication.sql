{%- set social_networks = ["instagram", "tiktok"] -%}

{%- set instagram_specific_fields = [
    "publication_saved",
    "publication_impressions",
    "publication_follows",
    "publication_profile_visits",
    "publication_profile_activity",
] -%}

{%- set tiktok_specific_fields = [
    "publication_video_duration",
    "publication_full_video_watched_rate",
] -%}

{{ config(materialized="table", cluster_by=["publication_account_name"]) }}

{% for social_network in social_networks %}
    select
        -- Partition and cluster fields
        publication_export_date,
        publication_account_name,

        -- Primary identifiers
        publication_id,
        publication_creation_date,

        -- Core metrics
        publication_reach,
        publication_video_views,
        publication_likes,
        publication_shares,
        publication_comments,
        publication_engagement_rate,

        -- Content information
        publication_caption,
        publication_link,
        is_publication_tagged,
        {% if social_network == "instagram" %} instagram_publication_media_type
        {% else %} null as instagram_publication_media_type
        {% endif %},

        -- Campaign tags
        publication_tag_post_name,
        publication_tag_macro_objective,
        publication_tag_micro_objective,
        publication_tag_offer_category,
        publication_tag_region_name,
        publication_tag_event_name,

        -- Instagram specific metrics
        {% if social_network == "instagram" %} total_instagram_publication_interactions
        {% else %} null as total_instagram_publication_interactions
        {% endif %},
        {% for field in instagram_specific_fields %}
            {% if social_network == "instagram" %} instagram_{{ field }}
            {% else %} null
            {% endif %} as instagram_{{ field }},
        {% endfor %}

        -- Tiktok specific metrics
        {% if social_network == "tiktok" %} average_tiktok_publication_time_watched
        {% else %} null as average_tiktok_publication_time_watched
        {% endif %},
        {% if social_network == "tiktok" %} total_tiktok_publication_time_watched
        {% else %} null as total_tiktok_publication_time_watched
        {% endif %},
        {% for field in tiktok_specific_fields %}
            {% if social_network == "tiktok" %} tiktok_{{ field }}
            {% else %} null
            {% endif %} as tiktok_{{ field }}
            {%- if not loop.last %},{% endif %}
        {% endfor %}

    from {{ ref("int_" ~ social_network ~ "__publication_detail") }}
    where
        publication_export_date = (
            select
                max(
                    social_network_publication_detail.publication_export_date
                ) as max_export_date
            from
                {{ ref("int_" ~ social_network ~ "__publication_detail") }}
                as social_network_publication_detail
        )

    {% if not loop.last %}
        union all
    {% endif %}

{% endfor %}
