/* This file is auto-generated */
/* TODO: reorganise into concepts proper to tracking */

{% docs column__event_date %}The date when the event occurred, used for partitioning the data.{% enddocs %}
{% docs column__event_name %}The name of the event. For screen views, it concatenates the event name with the Firebase screen name.{% enddocs %}
{% docs column__page_name %}The name of the page where the event occurred, providing context for the event.{% enddocs %}
{% docs column__event_timestamp %}The exact timestamp when the event was recorded.{% enddocs %}
{% docs column__user_pseudo_id %}A pseudo identifier for the user, used for tracking purposes.{% enddocs %}
{% docs column__origin %}The origin of the event, indicating where it was triggered from.{% enddocs %}
{% docs column__platform %}The platform on which the event was recorded, such as iOS or Android.{% enddocs %}
{% docs column__app_version %}The version of the application where the event was recorded.{% enddocs %}
{% docs column__traffic_campaign %}The campaign associated with the traffic that led to the event.{% enddocs %}
{% docs column__traffic_source %}The source of the traffic that led to the event.{% enddocs %}
{% docs column__traffic_medium %}The medium through which the traffic was acquired.{% enddocs %}
{% docs column__session_id %}Deprecated: use unique_session_id. The session identifier during which the event was recorded.{% enddocs %}
{% docs column__unique_session_id %}A unique identifier for the session, ensuring no duplicates.{% enddocs %}
{% docs column__session_number %}The number of the session during which the visit occurred, indicating the sequence of user sessions.{% enddocs %}
{% docs column__first_event_date %}The date of the first event in the session, used for partitioning the data.{% enddocs %}
{% docs column__first_event_timestamp %}The exact timestamp of the first event in the session.{% enddocs %}
{% docs column__last_event_timestamp %}The exact timestamp of the last event in the session.{% enddocs %}
{% docs column__visit_duration_seconds %}The duration of the visit in seconds, calculated from the first to the last event.{% enddocs %}
{% docs column__destination %}The destination associated with the event, if applicable, indicating the target location or page.{% enddocs %}
{% docs column__user_device_category %}The category of the user's device, such as mobile, tablet, or desktop.{% enddocs %}
{% docs column__user_device_operating_system %}The operating system of the user's device, such as Windows, macOS, iOS, or Android.{% enddocs %}
{% docs column__user_device_operating_system_version %}The version of the operating system on the user's device.{% enddocs %}
{% docs column__user_web_browser %}The web browser used by the user, such as Chrome, Firefox, or Safari.{% enddocs %}
{% docs column__user_web_browser_version %}The version of the web browser used by the user.{% enddocs %}
{% docs column__page_location %}The location of the page where the event occurred, typically a URL or path.{% enddocs %}
{% docs column__url_path_extract %}The extracted path from the URL where the event occurred.{% enddocs %}
{% docs column__page_referrer %}The referrer page that led to the current page where the event occurred.{% enddocs %}
{% docs column__page_number %}The number of the page in a sequence, if applicable.{% enddocs %}
