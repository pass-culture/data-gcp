---
title: Social Network Publication
description: Description of the `mrt_marketing__social_network_publication` table.
---

{% docs description__mrt_marketing__social_network_publication %}

The `mrt_marketing__social_network_publication` contains unified metrics and information about social media publications from both Instagram and TikTok platforms. Each row represents a unique social media post with its associated metrics, engagement data, and campaign tags.

Key components:
- Publication identifiers and basic information (ID, creation date, caption, URL)
- Core engagement metrics (reach, likes, comments, shares)
- Platform-specific metrics (Instagram: saves, profile visits; TikTok: watch time, video duration)
- Campaign tagging information (post name, objectives, offer category)

This mart table combines data from both Instagram and TikTok intermediate tables, providing a unified view of social media performance across platforms. The data is partitioned by export date and clustered by account name for optimal query performance.

For each account and publication ID, the table keeps the most recent observation available across all exports. Publications missing from the latest export remain present. Their metrics reflect their last available export, identified by `publication_export_date`, and are not necessarily up to date with the latest export.

{% enddocs %}

## Table description

{% docs table__mrt_marketing__social_network_publication %}{% enddocs %}
