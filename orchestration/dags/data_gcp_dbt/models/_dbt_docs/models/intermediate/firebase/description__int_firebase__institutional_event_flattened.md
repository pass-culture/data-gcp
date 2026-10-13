---
title: Institutional Event Flattened
description: Description of the `int_firebase__institutional_event_flattened` table.
---

{% docs description__int_firebase__institutional_event_flattened %}

# Table: Institutional Event Flattened

The `int_firebase__institutional_event_flattened` table stores every Google Analytics 4 event tracked on the institutional website (pass.culture.fr), one row per event, with the event parameters flattened into columns.

{% enddocs %}

## Table description

The table is built from the raw GA4 export of the institutional website (`raw.firebase_institutional_events`). It is imported daily from the `pc-site-instit-production` project, located in europe-west9, through a staging table copied to europe-west1.

Each row is an event: page views (`page_view`, `pageView`), sessions (`session_start`, `first_visit`, `user_engagement`), clicks and scrolls, and custom events such as `downloadApp`, `goToLoginNative`, `goToSignUpPro` or `consent.answer`.

The website is public: users are identified by browser only (`user_pseudo_id`) and `user_id` is always empty. A session is identified by the pair (`user_pseudo_id`, `ga_session_id`). There is no event-level unique key in the GA4 export. A few events come from the testing website: filter on `device_hostname = 'pass.culture.fr'` to keep production traffic only.

The table is incremental and partitioned on `event_date`: each run overwrites the last 3 days.

{% docs table__int_firebase__institutional_event_flattened %}{% enddocs %}
