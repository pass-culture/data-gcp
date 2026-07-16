The `mrt_marketing__social_network_publication` contains unified metrics and information about social media publications from both Instagram and TikTok platforms. Each row represents a unique social media post with its associated metrics, engagement data, and campaign tags.

Key components:

- Publication identifiers and basic information (ID, creation date, caption, URL)
- Core engagement metrics (reach, likes, comments, shares)
- Platform-specific metrics (Instagram: saves, profile visits; TikTok: watch time, video duration)
- Campaign tagging information (post name, objectives, offer category)

This mart table combines data from both Instagram and TikTok intermediate tables, providing a unified view of social media performance across platforms. The data is partitioned by export date and clustered by account name for optimal query performance.

## Table description

| name                                       | data_type | description                                                                                                                   |
| ------------------------------------------ | --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| publication_id                             | STRING    | The unique identifier for the publication. Format varies by platform: numeric ID for Instagram, alphanumeric for TikTok.      |
| publication_creation_date                  | DATE      | The date when the content was originally posted on the social media platform.                                                 |
| publication_caption                        | STRING    | The text content accompanying the publication, including hashtags, mentions, and emojis.                                      |
| publication_link                           | STRING    | The permanent URL to access the publication directly on the social media platform.                                            |
| publication_export_date                    | DATE      | The date when the publication's metrics were extracted from the platform.                                                     |
| publication_account_name                   | STRING    | The identifier of the account that posted the content, prefixed with the platform name (e.g., instagram\_, tiktok\_).         |
| is_publication_tagged                      | BOOLEAN   | Boolean indicator (TRUE/FALSE) showing whether the publication is part of a tracked campaign.                                 |
| publication_reach                          | INT64     | The number of unique accounts that saw the publication at least once.                                                         |
| publication_video_views                    | INT64     | The total number of times the video content was played. Only applicable for video content.                                    |
| publication_likes                          | INT64     | The number of unique accounts that liked the publication.                                                                     |
| publication_shares                         | INT64     | The number of times the publication was shared by users to their stories or with other users.                                 |
| publication_comments                       | INT64     | The total number of comments received on the publication.                                                                     |
| publication_engagement_rate                | FLOAT64   | The ratio of total interactions (likes + comments + shares) to reach.                                                         |
| publication_tag_post_name                  | STRING    | The unique identifier assigned to the publication for campaign tracking purposes.                                             |
| publication_tag_macro_objective            | STRING    | The high-level marketing goal of the publication.                                                                             |
| publication_tag_micro_objective            | STRING    | The specific marketing objective of the publication.                                                                          |
| publication_tag_offer_category             | STRING    | The category of the offer associated with the publication.                                                                    |
| publication_tag_region_name                | STRING    | The region name associated with the publication.                                                                              |
| publication_tag_event_name                 | STRING    | The name of the event associated with the publication.                                                                        |
| instagram_publication_media_type           | STRING    | The format of the Instagram publication content. Possible values include: PHOTO, VIDEO, CAROUSEL_ALBUM                        |
| instagram_publication_saved                | INT64     | The number of times the publication was saved by users.                                                                       |
| total_instagram_publication_interactions   | INT64     | The sum of all engagement actions on the publication, including: Likes, Comments, Shares, Saves, Video views (if applicable). |
| instagram_publication_impressions          | INT64     | The total number of times the publication was viewed.                                                                         |
| instagram_publication_follows              | INT64     | The number of new followers acquired directly through this publication.                                                       |
| instagram_publication_profile_visits       | INT64     | The number of times users visited the profile after seeing this publication.                                                  |
| instagram_publication_profile_activity     | INT64     | The total number of actions taken on the profile from this publication, including: Website clicks, Email button clicks...     |
| average_tiktok_publication_time_watched    | FLOAT64   | The average duration (in seconds) that viewers spent watching the video.                                                      |
| total_tiktok_publication_time_watched      | INT64     | The cumulative time (in seconds) spent by all viewers watching the video.                                                     |
| tiktok_publication_video_duration          | FLOAT64   | The total length of the video content in seconds.                                                                             |
| tiktok_publication_full_video_watched_rate | FLOAT64   | The percentage of viewers who watched the entire video from start to finish.                                                  |
