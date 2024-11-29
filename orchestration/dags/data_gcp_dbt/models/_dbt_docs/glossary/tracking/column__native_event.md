{% docs column__event_date %}The date when the event occurred, used for partitioning the data.{% enddocs %}
{% docs column__event_name %}The name of the event. For screen views, it concatenates the event name with the Firebase screen name.{% enddocs %}
{% docs column__platform %}The platform on which the event was recorded (iOS, Android, webapp).{% enddocs %}
{% docs column__firebase_screen %}The name of the screen where the event occurred, providing context for the event.{% enddocs %}
{% docs column__event_timestamp %}The exact timestamp when the event was recorded.{% enddocs %}
{% docs column__user_pseudo_id %}A pseudo identifier for the user, used for tracking purposes.{% enddocs %}
{% docs column__origin %}The origin of the event, indicating where it was triggered from.{% enddocs %}
{% docs column__app_version %}The version of the application where the event was recorded.{% enddocs %}
{% docs column__traffic_campaign %}The name of the marketing campaign that generated the session.{% enddocs %}
{% docs column__traffic_source %}The source of the marketing campaign (Instagram, Snapchat ...) that generated the session.{% enddocs %}
{% docs column__traffic_medium %}The medium of the marketing campaign (email, push notification ...)that generated the session.{% enddocs %}
{% docs column__session_id %}Deprecated: use unique_session_id. The session identifier during which the event was recorded.{% enddocs %}
{% docs column__unique_session_id %}A unique identifier for the session, ensuring no duplicates.{% enddocs %}
{% docs column__user_location_type %}The type of geolocation used by the user during the session (via geolocation data, manually entered by the user, or information unavailable).{% enddocs %}
