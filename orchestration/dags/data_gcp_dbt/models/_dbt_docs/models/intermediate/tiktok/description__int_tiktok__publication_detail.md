---
title: Publication Detail
description: Description of the `int_tiktok__publication_detail` table.
---

{% docs description__int_tiktok__publication_detail %}

The `int_tiktok__publication_detail` contains detailed metrics and information about TikTok publications, including reach, engagement metrics, and campaign tagging data. Each row represents a unique TikTok post with its associated metrics and tags.

Key components:
- Publication identifiers and basic information (ID, creation date, caption, URL)
- Engagement metrics (views, likes, shares, comments)
- Video-specific metrics (average watch time, total watch time, video duration)
- Campaign tagging information (post name, objectives, offer category)

This intermediate table joins raw TikTok video data with campaign tagging information from Google Sheets, enabling analysis of both organic and campaign performance. The data is partitioned by export date and clustered by account name for optimal query performance.

{% enddocs %}

## Table description

{% docs table__int_tiktok__publication_detail %}{% enddocs %}
