---
title: Publication detail
description: Description of the `int_instagram__publication_detail` table.
---

{% docs description__int_instagram__publication_detail %}

The `int_instagram__publication_detail` contains detailed metrics and information about Instagram publications, including reach, engagement metrics, and campaign tagging data. Each row represents a unique Instagram post with its associated metrics and tags.

Key components:
- Publication identifiers and basic information (ID, creation date, caption, URL, media type)
- Engagement metrics (reach, likes, comments, shares, saves)
- Instagram-specific metrics (impressions, profile visits, follows, profile activity)
- Campaign tagging information (post name, objectives, offer category)

This intermediate table joins raw Instagram post data with campaign tagging information from Google Sheets, enabling analysis of both organic and campaign performance. The data is partitioned by export date and clustered by account name for optimal query performance.

{% enddocs %}

## Table description

{% docs table__int_instagram__publication_detail %}{% enddocs %}
