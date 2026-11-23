---
title: Institutional Event
description: Description of the `mrt_institutional__event` table.
---

{% docs description__mrt_institutional__event %}

The `mrt_institutional__event` table exposes event-level Google Analytics data from the pass.culture.fr website for audience, acquisition, navigation, engagement, and publication-download analysis.

The table contains one row per recorded event. It keeps raw session and first-touch source/medium values for flexible acquisition analysis; no channel taxonomy is applied. `session_id` is the GA4 session identifier, and `unique_session_id` combines it with `user_pseudo_id`.

The model is restricted to the production hostname and partitioned by event date. Event-level uniqueness is not asserted because the GA4 export does not provide a unique event key.

{% enddocs %}

## Table description

{% docs table__mrt_institutional__event %}{% enddocs %}
