{% docs column__count_duplicate_event_series_id_in_future_event_series %}Number of duplicate `event_series_id` entries in the future event series data.{% enddocs %}
{% docs column__count_null_or_empty_event_series_name_in_future_event_series %}Number of rows with a null or empty `event_series_name` in the future event series data.{% enddocs %}
{% docs column__count_duplicate_links_in_future_event_series_offer_link %}Number of duplicate `(offer_id, event_series_id)` pairs in the future event series offer link data.{% enddocs %}
{% docs column__count_orphan_event_series_in_future_event_series_offer_link %}Number of future event series offer links whose `event_series_id` does not exist in the future event series data.{% enddocs %}
{% docs column__count_null_or_empty_event_series_id_in_future_event_series %}Number of rows with a null or empty `event_series_id` in the future event series data.{% enddocs %}
{% docs column__count_null_or_empty_keys_in_future_event_series_offer_link %}Number of rows with a null or empty `event_series_id` or `offer_id` in the future event series offer link data.{% enddocs %}
{% docs column__count_invalid_actions_in_event_series_delta %}Number of rows in the event series delta with a null action or an action other than `add`, `update`, or `remove`.{% enddocs %}
{% docs column__count_invalid_actions_in_event_series_offer_link_delta %}Number of rows in the event series offer link delta with a null action or an action other than `add`, `update`, or `remove`.{% enddocs %}
{% docs column__count_multi_event_series_offers_in_future %}Number of offers linked to more than one event series in the future event series offer link data.{% enddocs %}
{% docs column__count_missing_mediation_uuid_in_event_series_delta %}Number of added or updated rows in the event series delta with an image url but a null or empty mediation uuid.{% enddocs %}
{% docs column__count_null_or_empty_event_series_id_in_event_series_delta %}Number of rows with a null or empty `event_series_id` in the event series delta.{% enddocs %}
{% docs column__count_null_or_empty_event_series_name_in_event_series_delta %}Number of rows with a null or empty `event_series_name` in the event series delta.{% enddocs %}
{% docs column__is_future_event_series_below_shrinkage_threshold %}True when the future event series row count drops below 30% of the current applicative event series row count.{% enddocs %}
{% docs column__is_future_event_series_offer_link_below_shrinkage_threshold %}True when the future event series offer link row count drops below 30% of the current applicative event series offer link row count.{% enddocs %}
{% docs column__ready_for_ingestion %}Boolean flag that is true when all check counts are zero and no shrinkage threshold is breached, indicating the data is ready for backend ingestion.{% enddocs %}
