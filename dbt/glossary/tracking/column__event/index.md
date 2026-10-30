Note

This file is auto-generated

Tracking that are not related to specific concepts for now

Work in progress

**event_date**: The date when the event occurred, used for partitioning the data.

**event_name**: The name of the event. For screen views, it concatenates the event name with the Firebase screen name.

**page_name**: The name of the page where the event occurred, providing context for the event.

**event_timestamp**: The exact timestamp when the event was recorded.

**user_pseudo_id**: Firebase pseudonymous identifier of the app instance (the device, or the browser for web tracking), used for tracking purposes. One person can have several, and people sharing a device share one.

**origin**: The origin of the event, indicating where it was triggered from.

**origin_detail**: The details of the origin of the event.

**platform**: The platform on which the event was recorded, such as iOS or Android.

**app_version**: The version of the application where the event was recorded.

**radio_type**: Type of network connection (e.g., WIFI, LTE, 5G).

**traffic_campaign**: The name of the marketing campaign that generated the session.

**traffic_source**: The source of the marketing campaign (Instagram, Snapchat ...) that generated the session.

**traffic_medium**: The medium of the marketing campaign (email, push notification ...)that generated the session.

**session_id**: Deprecated: use unique_session_id. Firebase session number, unique only within a device (user_pseudo_id).

**adage_log_session_id**: Session identifier computed from the ADAGE logs: consecutive logs of the same user at most 30 minutes apart belong to the same session. MD5 hash of the session start, the user_id and the session number.

**unique_session_id**: A unique identifier for the session, ensuring no duplicates.

**consultation_module_tag_name**: Name of the Algolia/home module tag associated with the consultation, when the consultation originated from a tagged module. Null otherwise.

**consultation_user_role**: Role of the user at the time of the event, derived from their deposit status: 'Bénéficiaire' (has an active deposit), 'Ancien bénéficiaire' (deposit expired), 'Non bénéficiaire' (logged in, no deposit), or 'Non loggué' (not logged in).

**consultation_user_age**: Age of the user at the time of the event, computed from their birth date and the event date.
