!!! note
This file is auto-generated

Tracking that are not related to specific concepts for now

:construction_worker_tone1: Work in progress :construction_worker_tone1:


{% docs column__event_date %}The date when the event occurred, used for partitioning the data.{% enddocs %}
{% docs column__event_name %}The name of the event. For screen views, it concatenates the event name with the Firebase screen name.{% enddocs %}
{% docs column__page_name %}The name of the page where the event occurred, providing context for the event.{% enddocs %}
{% docs column__event_timestamp %}The exact timestamp when the event was recorded.{% enddocs %}
{% docs column__user_pseudo_id %}A pseudo identifier for the user, used for tracking purposes.{% enddocs %}
{% docs column__origin %}The origin of the event, indicating where it was triggered from.{% enddocs %}
{% docs column__platform %}The platform on which the event was recorded, such as iOS or Android.{% enddocs %}
{% docs column__app_version %}The version of the application where the event was recorded.{% enddocs %}
{% docs column__radio_type %}Type of network connection (e.g., WIFI, LTE, 5G).{% enddocs %}
{% docs column__traffic_campaign %}The name of the marketing campaign that generated the session.{% enddocs %}
{% docs column__traffic_source %}The source of the marketing campaign (Instagram, Snapchat ...) that generated the session.{% enddocs %}
{% docs column__traffic_medium %}The medium of the marketing campaign (email, push notification ...)that generated the session.{% enddocs %}
{% docs column__session_id %}Deprecated: use unique_session_id. The session identifier during which the event was recorded.{% enddocs %}
{% docs column__unique_session_id %}A unique identifier for the session, ensuring no duplicates.{% enddocs %}


{% hide columns %}

{% docs column__session_number %}The number of the session during which the visit occurred, indicating the sequence of user sessions.{% enddocs %}
{% docs column__time_to_interactive %}Time To Interactive (TTI) in milliseconds for the home container; it is the total time between the app launch and the moment when the homepage is usable.{% enddocs %}
{% docs column__first_event_date %}The date of the first event in the session, used for partitioning the data.{% enddocs %}
{% docs column__first_event_timestamp %}The exact timestamp of the first event in the session.{% enddocs %}
{% docs column__last_event_timestamp %}The exact timestamp of the last event in the session.{% enddocs %}
{% docs column__visit_duration_seconds %}The duration of the visit in seconds, calculated from the first to the last event.{% enddocs %}
{% docs column__destination %}The destination associated with the event, if applicable, indicating the target location or page.{% enddocs %}
{% docs column__user_device_category %}The category of the user's device, such as mobile, tablet, or desktop.{% enddocs %}
{% docs column__user_device_name %}Name or model of the device.{% enddocs %}
{% docs column__user_device_operating_system %}The operating system of the user's device, such as Windows, macOS, iOS, or Android.{% enddocs %}
{% docs column__user_device_operating_system_version %}The version of the operating system on the user's device.{% enddocs %}
{% docs column__user_web_browser %}The web browser used by the user, such as Chrome, Firefox, or Safari.{% enddocs %}
{% docs column__user_web_browser_version %}The version of the web browser used by the user.{% enddocs %}
{% docs column__user_carrier %}Mobile carrier or network provider.{% enddocs %}
{% docs column__user_country %}Country of the user.{% enddocs %}
{% docs column__page_location %}The location of the page where the event occurred, typically a URL or path.{% enddocs %}
{% docs column__url_path_extract %}The extracted path from the URL where the event occurred.{% enddocs %}
{% docs column__page_referrer %}The referrer page that led to the current page where the event occurred.{% enddocs %}
{% docs column__page_number %}The number of the page in a sequence, if applicable.{% enddocs %}
{% docs column__event_type %}The type of the interaction event (booking / click / favorite).{% enddocs %}
{% docs column__event_hour %}The hour of the interaction event.{% enddocs %}
{% docs column__event_day %}The day of the interaction event.{% enddocs %}
{% docs column__event_month %}The month of the interaction event.{% enddocs %}


{% endhide %}
