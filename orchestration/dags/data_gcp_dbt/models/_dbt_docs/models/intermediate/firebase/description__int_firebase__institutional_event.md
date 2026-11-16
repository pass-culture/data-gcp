---
title: Institutional Event
description: Description of the `int_firebase__institutional_event` table.
---

{% docs description__int_firebase__institutional_event %}

This model contains Google Analytics events recorded on the production pass.culture.fr website. It retains all flattened event data and adds a session identifier based on the user and session IDs.

{% enddocs %}

## Table description

{% docs table__int_firebase__institutional_event %}{% enddocs %}

{% docs column__institutional_event_session_id %}Google Analytics session identifier. It is unique only when combined with `user_pseudo_id`; use `unique_session_id` as the composite session key.{% enddocs %}
