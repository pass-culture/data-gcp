{% docs column__reco_total_bookings %} Total number of bookings for a given item. {% enddocs %}
{% docs column__reco_total_bookings_last_7_days %} Total number of bookings for a given item made during the last 7 days. {% enddocs %}
{% docs column__reco_total_bookings_last_14_days %} Total number of bookings for a given item made during the last 14 days. {% enddocs %}
{% docs column__reco_total_bookings_last_28_days %} Total number of bookings for a given item made during the last 28 days. {% enddocs %}
{% docs column__reco_item_is_underage_recommendable %} Indicates if the item is recommendable for underage users. {% enddocs %}
{% docs column__reco_item_is_sensitive %} Indicates if the item is considered sensitive. {% enddocs %}
{% docs column__reco_item_is_restrained %} Indicates if the item is restrained. {% enddocs %}
{% docs column__reco_total_offers %} Indicates the number of offers for a give item. {% enddocs %}
{% docs column__reco_mean_semantic_embedding %} Mean value of the semantic embedding for a given item. {% enddocs %}
{% docs column__reco_booking_trend %} Trend of bookings for a given item. {% enddocs %}
{% docs column__reco_stock_date_penalty_factor %} Penalty factor based on the stock date of the item. {% enddocs %}
{% docs column__reco_creation_date_penalty_factor %} Penalty factor based on the creation date of the item. {% enddocs %}
{% docs column__reco_example_offer_name %} Example name of an offer for the item. {% enddocs %}
{% docs column__reco_example_offer_id %} Example ID of an offer for the item. {% enddocs %}
{% docs column__reco_example_venue_id %} Example ID of a venue for the item. {% enddocs %}
{% docs column__reco_example_venue_longitude %} Longitude of the example venue for the item. {% enddocs %}
{% docs column__reco_example_venue_latitude %} Latitude of the example venue for the item. {% enddocs %}
{% docs column__reco_booking_release_trend %} Trend of booking releases for a given item. {% enddocs %}
{% docs column__reco_booking_creation_trend %} Trend of booking creations for a given item. {% enddocs %}
{% docs column__qpi_ux_name %} Name of the UX category shown to users in the QPI onboarding flow (for example, "Mangas ou BD"). {% enddocs %}
{% docs column__qpi_backend_name %} Backend identifier ("Nom BDD") associated to the UX category (for example, "MANGAS"). Values ending in "_AUCUN" (`LIVRES_AUCUN`, `MUSIQUES_AUCUN`) correspond to the "Aucun" rows. {% enddocs %}
{% docs column__qpi_gtl_label %} Titelive GTL label (at any level, 1 to 4) grouped under the corresponding `ux_name`, as declared in the static QPI mapping. Null for the "Aucun" rows. {% enddocs %}
{% docs column__qpi_gtl_label_matched %} Titelive GTL label actually resolved in `int_applicative__titelive_gtl` for `gtl_label`. Null when `gtl_label` could not be matched. {% enddocs %}
{% docs column__qpi_matched_gtl_level %} GTL hierarchy level (1 to 4) at which `gtl_label` was matched in `int_applicative__titelive_gtl`. Null when `gtl_label` could not be resolved. {% enddocs %}
