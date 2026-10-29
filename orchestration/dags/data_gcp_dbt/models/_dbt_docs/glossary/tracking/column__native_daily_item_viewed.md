{% docs column__native_daily_item_viewed_event_date %}Date of the view. Partition column: a partition filter is required. Data starts on 2025-06-02 (trackers partial before 2025-11-01) and each run loads the day three days back, so the most recent days are not yet available.{% enddocs %}

{% docs column__native_daily_item_viewed_origin %}App channel where the item was displayed. Stored values: 'home', 'searchresults' (text search), 'thematicsearch' (thematic search), 'venue' (venue page), 'venue_map' (map), 'artist' (artist page), 'offer' (offer page, similar offers).{% enddocs %}

{% docs column__native_daily_item_viewed_module_id %}Contentful identifier of the module (block) that displayed the item; NULL outside a module.{% enddocs %}

{% docs column__native_daily_item_viewed_home_entry_id %}Contentful entry identifier of the home where the item was displayed; NULL outside the home.{% enddocs %}

{% docs column__native_daily_item_viewed_playlist_name %}Name of the module (Contentful title, or offer title when there is none) identified by module_id; NULL when the module has no Contentful tag.{% enddocs %}

{% docs column__native_daily_item_viewed_playlist_offer_category %}Offer categories the module is tagged with in Contentful (tag 'categorie_offre'), several values joined with ' , '.{% enddocs %}

{% docs column__native_daily_item_viewed_item_type %}Type of the displayed item: 'offer' (viewed_item_id is an offer_id), 'venue' (viewed_item_id is a venue_id), and other types such as 'artist'.{% enddocs %}

{% docs column__native_daily_item_viewed_viewed_item_id %}Identifier of the displayed item: an offer_id when item_type = 'offer', a venue_id when item_type = 'venue', an artist_id when item_type = 'artist'.{% enddocs %}

{% docs column__native_daily_item_viewed_user_role %}Role of the user from the applicative user table when the day was processed (about three days after the view): 'BENEFICIARY', 'UNDERAGE_BENEFICIARY', 'FREE_BENEFICIARY'; NULL for general public, other roles or users not logged in.{% enddocs %}

{% docs column__native_daily_item_viewed_total_user_item_viewed %}Number of distinct users who saw the item that day for the row's channel, module and user attributes, summed over the four display-position buckets (a user seeing the item in two position buckets counts twice).{% enddocs %}

{% docs column__native_daily_item_viewed_total_user_item_viewed_bucket_1_3 %}Number of distinct logged-in users who saw the item in positions 1 to 3 of its module (item index 0 to 2) that day, for the row's attributes.{% enddocs %}

{% docs column__native_daily_item_viewed_total_user_item_viewed_bucket_4_6 %}Number of distinct logged-in users who saw the item in positions 4 to 6 of its module (item index 3 to 5) that day, for the row's attributes.{% enddocs %}

{% docs column__native_daily_item_viewed_total_user_item_viewed_bucket_7_9 %}Number of distinct logged-in users who saw the item in positions 7 to 9 of its module (item index 6 to 8) that day, for the row's attributes.{% enddocs %}

{% docs column__native_daily_item_viewed_total_user_item_viewed_bucket_10_plus %}Number of distinct logged-in users who saw the item in position 10 or beyond of its module (item index 9 or more) that day, for the row's attributes. Also counts views with an unknown position (index -1).{% enddocs %}
