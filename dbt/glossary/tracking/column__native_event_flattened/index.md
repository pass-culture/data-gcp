**native_event_flattened_event_date**: Date of the Firebase event. Partition column: a partition filter is required (the table holds several terabytes).

**native_event_flattened_user_id**: Identifier of the logged-in pass Culture account that emitted the event; NULL when the user is not logged in. Use it, not user_pseudo_id, to count people.

**native_event_flattened_user_pseudo_id**: Firebase app-instance (device) pseudo identifier. One person can have several; it does not identify a person.

**native_event_flattened_event_name**: Raw Firebase event name (e.g. 'ModuleDisplayedOnHomePage', 'screen_view', 'user_engagement', 'ViewItem', 'ConsultHome', 'PerformSearch', 'ConsultOffer', 'session_start'). All events are kept except repeated 'ConsultOffer' on the same offer within 3 seconds in the same session, which are deduplicated.

**native_event_flattened_event_previous_timestamp**: Time at which the same event was previously logged by this app instance (Firebase `event_previous_timestamp`, converted from microseconds).

**native_event_flattened_user_first_touch_timestamp**: Timestamp of the first time the user opened the app on this app instance (Firebase `user_first_touch_timestamp`). Partitions built before the October 2026 fix hold the event timestamp instead.

**native_event_flattened_platform**: Platform that emitted the event. Stored values: 'IOS', 'ANDROID', 'WEB'.

**native_event_flattened_double_offer_id**: Value of the `offerId` event parameter when the app sent it as a double; NULL otherwise. The string and integer forms of the parameter are in offerId.

**native_event_flattened_ga_session_id**: Firebase session identifier, unique only within a device: the unique session key is the pair (user_pseudo_id, ga_session_id).

**native_event_flattened_should_use_algolia_recommend**: Integer value of the Firebase event parameter `shouldUseAlgoliaRecommend`: flag telling whether the module relied on Algolia Recommend for its recommendations (1 = yes).

**native_event_flattened_search_is_autocomplete**: On 'PerformSearch', 1 when the search was launched from an autocomplete suggestion; NULL otherwise (the app only sends the parameter when it is true).

**native_event_flattened_search_is_based_on_history**: On 'PerformSearch', 1 when the search was launched from the user's search history; NULL otherwise (the app only sends the parameter when it is true).

**native_event_flattened_search_offer_is_duo**: On 'PerformSearch', 1 when the 'Duo' filter (offers bookable for two people) is on; NULL otherwise (the app only sends the parameter when it is true).

**native_event_flattened_geo_located**: Recommendation API parameter sent with recommendation modules: 1 when the recommendation was computed with the user's location, 0 otherwise. Merges the `geoLocated` and `geo_located` event parameters.

**native_event_flattened_enabled**: On 'ChangeOrientationToggle', the new state of the screen orientation toggle: 1 when enabled, 0 when disabled.

**native_event_flattened_search_id**: Identifier of a search. One search emits several 'PerformSearch' events as filters change, all sharing the same searchId. NULL on a small share of events.

**native_event_flattened_module_id**: Identifier of the home module (block) carried by the event, when relevant.

**native_event_flattened_playlist_type**: Type of the offer playlist the event comes from, e.g. 'sameCategorySimilarOffers', 'otherCategoriesSimilarOffers', 'booksSameCategorySimilarOffers', 'sameArtistPlaylist', 'artistTopOffers', 'searchResults'.

**native_event_flattened_entry_id**: Contentful entry identifier carried by the event: the home or block on which the event happened (e.g. on 'ViewItem', 'CategoryBlockClicked', 'HighlightBlockClicked').

**native_event_flattened_home_entry_id**: Contentful entry identifier of the home on which the event happened, sent on home events such as 'ConsultHome', 'ModuleDisplayedOnHomePage' or 'SeeAllClicked'.

**native_event_flattened_step**: Funnel step parameter. On 'CancelBookingFunnel', the booking modal step the user closed; the signup and identity-check funnel events carry their own step names in the same column.

**native_event_flattened_search_genre_types**: On 'PerformSearch', the genre filters selected (e.g. book or music genres), as a JSON array in a string.

**native_event_flattened_search_location_filter**: On 'PerformSearch', the location filter as a JSON string: the location type and, for a place or a venue, its label (truncated so that the parameter stays within 100 characters).

**native_event_flattened_age**: Age chosen or entered by the user, sent on onboarding and sign-up events such as 'SelectAge' or 'SignUpTooYoung'.

**native_event_flattened_search_categories**: On 'PerformSearch', the category filters selected, as a JSON array in a string.

**native_event_flattened_firebase_screen**: Name of the screen displayed, set on 'screen_view' events.

**native_event_flattened_firebase_previous_screen**: Name of the screen displayed before firebase_screen, set on 'screen_view' events.

**native_event_flattened_page_name**: Name of the page the event refers to, e.g. on 'CancelSignup' the sign-up page the user left.

**native_event_flattened_offer_id**: Identifier of the offer carried by the event (consultation, booking, favorite...).

**native_event_flattened_query**: Firebase event parameter `query`: the search text sent by search events other than 'PerformSearch', such as 'NoSearchResult'. On 'PerformSearch' the text is in searchQuery.

**native_event_flattened_search_query**: Text typed by the user for a search ('PerformSearch'); NULL for a search without text (category or location filters only).

**native_event_flattened_category_name**: Name of the offer category sent with club and review events, such as 'ClickAllClubRecos', 'ClickWhatsClub' or 'ClickInfoReview'.

**native_event_flattened_type**: Generic Firebase event parameter `type`, whose meaning depends on the event: the shared content on 'Share' ('Artist', 'Offer', 'Venue', 'App'), the kind of list on 'SeeAllClicked' ('offers', 'venues', 'artists', 'categories'), the block on 'SystemBlockDisplayed', the filter type on search filter events, etc.

**native_event_flattened_venue_id**: Identifier of the venue carried by the event.

**native_event_flattened_booking_id**: Identifier of the confirmed booking, set on 'BookingConfirmation' events; matches booking_id in the applicative booking tables.

**native_event_flattened_from_offer_id**: Identifier of the offer whose playlist (similar offers) the user came from, set when an offer is consulted or booked from such a playlist; see playlistType.

**native_event_flattened_filter_types**: Firebase event parameter `filterTypes`: the types of search filters applied, as sent by the app.

**native_event_flattened_filter**: Firebase event parameter `filter`: the search filter the event refers to, as sent by the app.

**native_event_flattened_search_date**: On 'PerformSearch', the date filter as a JSON string with startDate, endDate and searchFilter (the date option chosen, 'dateInterval' or 'specificDate').

**native_event_flattened_search_max_price**: On 'PerformSearch', the maximum price filter, as a string; NULL when no maximum price is set.

**native_event_flattened_search_native_categories**: On 'PerformSearch', the native category filters selected (sub-levels of the categories), as a JSON array in a string.

**native_event_flattened_module_list_id**: On 'CategoryBlockClicked' and 'TrendsBlockClicked', the identifier of the list module that contains the clicked block.

**native_event_flattened_index**: Position sent with the event: on 'ModuleDisplayedOnHomePage', the position of the module on the home; on 'ViewItem', the position of the playlist on the page.

**native_event_flattened_traffic_gen**: `utm_gen` parameter of the marketing link that opened the app, attached by the app to the following events (only when the user accepted the marketing cookies).

**native_event_flattened_traffic_content**: `utm_content` parameter of the marketing link that opened the app, attached by the app to the following events (only when the user accepted the marketing cookies).

**native_event_flattened_to_entry_id**: On 'CategoryBlockClicked', 'TrendsBlockClicked' and 'HighlightBlockClicked', the Contentful entry identifier of the page (thematic home) opened by the click.

**native_event_flattened_reco_origin**: Recommendation API parameter sent with recommendation modules: origin of the recommendation. Merges the `recoOrigin` and `reco_origin` event parameters.

**native_event_flattened_ab_test**: Recommendation API parameter sent with recommendation modules: the A/B test group of the recommendation call. Merges the `abTest` and `ab_test` event parameters.

**native_event_flattened_model_version**: Recommendation API parameter sent with recommendation modules: version of the recommendation model. Merges the `modelVersion` and `model_version` event parameters.

**native_event_flattened_model_name**: Recommendation API parameter sent with recommendation modules: name of the recommendation model. Merges the `modelName` and `model_name` event parameters.

**native_event_flattened_model_endpoint**: Recommendation API parameter sent with recommendation modules: endpoint of the recommendation model that served the call. Merges the `modelEndpoint` and `model_endpoint` event parameters.

**native_event_flattened_user_status**: Firebase event parameter `userStatus`: the status chosen by the user during onboarding, as sent by the app.

**native_event_flattened_social**: On 'Share', the social network or share channel chosen by the user ('Other' when not listed).

**native_event_flattened_search_view**: On 'PerformSearch', the search screen the search was made from.

**native_event_flattened_duration**: On 'VenueMapSeenDuration' and 'VenueMapSessionDuration', the time spent on the venue map, in seconds (as a string).

**native_event_flattened_appsflyer_user_id**: AppsFlyer identifier of the app install, sent with the event to link Firebase and AppsFlyer data.

**native_event_flattened_accessibility_filter**: On 'PerformSearch', the accessibility filters as a JSON string with the keys 'Audio', 'Mental', 'Motor' and 'Visual' (true or false).

**native_event_flattened_video_duration**: Total duration of the video, in seconds (as a string), sent with video events such as 'HasSeenAllVideo' and 'HasDismissedModal'.

**native_event_flattened_seen_duration**: Time of the video watched by the user, in seconds (as a string), sent with video events such as 'HasSeenAllVideo' and 'HasDismissedModal'.

**native_event_flattened_youtube_id**: YouTube identifier of the video, sent with video events such as 'ConsultVideo' and 'HasSeenAllVideo'.

**native_event_flattened_origin**: Where the event was triggered from in the app: the `from` event parameter, or the `origin` parameter when `from` is absent (on 'ViewItem', the page in lower case, e.g. 'home', 'searchresults', 'venue').

**native_event_flattened_displayed_offers**: On 'ModuleDisplayedOnHomePage', identifiers of the offers displayed in the module, rebuilt as an array from the `offers_1_10` to `offers_41_50` parameters (first 50 offers at most); empty array when none.

**native_event_flattened_displayed_venues**: On 'ModuleDisplayedOnHomePage', identifiers of the venues displayed in the module, rebuilt as an array from the `venues_1_10` to `venues_41_50` parameters (first 50 venues at most); empty array when none.

**native_event_flattened_from_multivenue_offer_id**: Identifier of the multi-venue offer from which the user selected the offer of another venue; set on 'ConsultOffer'.

**native_event_flattened_is_anomaly**: TRUE for technically inconsistent events: 'BookingConfirmation' without bookingId or without user_id, and 'ConsultOffer' without offerId.

**native_event_flattened_is_headline**: On 'ConsultOffer', 'true' when the consulted offer is the headline offer of its venue, 'false' otherwise.

**native_event_flattened_items_0**: On 'ViewItem', the first chunk of the displayed items, as 'position:item_id' pairs separated by commas (100 characters at most; position -1 when unknown). Only this first chunk is extracted: the items sent in the following chunks (`items_1`, ...) are not in this table.

**native_event_flattened_item_type**: On 'ViewItem', the type of the displayed items.

**native_event_flattened_theme_setting**: On 'UpdateAppTheme' and 'AppThemeStatus', the theme chosen in the app settings (e.g. 'light', 'dark').

**native_event_flattened_system_theme**: On 'UpdateAppTheme' and 'AppThemeStatus', the theme of the device system (e.g. 'light', 'dark').

**native_event_flattened_legacy_geo_located**: Legacy column from an earlier version of the model, no longer filled: the `geoLocated` parameter is now merged into geo_located.

**native_event_flattened_legacy_call_id**: Legacy column from an earlier version of the model, no longer filled: the `callId` parameter is now merged into call_id.

**native_event_flattened_legacy_reco_origin**: Legacy column from an earlier version of the model, no longer filled: the `recoOrigin` parameter is now merged into reco_origin.

**native_event_flattened_legacy_ab_test**: Legacy column from an earlier version of the model, no longer filled: the `abTest` parameter is now merged into ab_test.

**native_event_flattened_legacy_model_version**: Legacy column from an earlier version of the model, no longer filled: the `modelVersion` parameter is now merged into model_version.

**native_event_flattened_legacy_model_name**: Legacy column from an earlier version of the model, no longer filled: the `modelName` parameter is now merged into model_name.

**native_event_flattened_legacy_model_endpoint**: Legacy column from an earlier version of the model, no longer filled: the `modelEndpoint` parameter is now merged into model_endpoint.

**native_event_flattened_feature_name**: Name of the feature the event refers to, on feature feedback and fake door events ('FeatureFeedbackClicked', 'HasClickedFakeDoorCTA').

**native_event_flattened_feedback_response**: On 'FeatureFeedbackClicked', the user's answer to the feature feedback question: 'Oui' or 'Non'.

**native_event_flattened_origin_details**: Detail completing origin on where the event was triggered from, e.g. on 'ConsultArtist', 'ConsultAdvice' or 'HasClickedFakeDoorCTA'.

**native_event_flattened_advice_type**: Type of advice (recommendation written by a club or a partner) the event refers to: 'book_club', 'cine_club', 'scene_club' or 'pro'.

**native_event_flattened_origin_detail**: On 'HasExitedActivationFlow', the call to action the user clicked to leave the activation flow.

**native_event_flattened_code**: Firebase `code` parameter. On `BookingError`, the API error that blocked the booking: 'ALREADY_BOOKED', 'INSUFFICIENT_CREDIT', 'STOCK_NOT_BOOKABLE', 'PROVIDER_STOCK_NOT_ENOUGH_SEATS', 'PROVIDER_SHOW_DOES_NOT_EXIST', 'PROVIDER_BOOKING_TIMEOUT'. Populated since 2026-07-05, NULL before.

**native_event_flattened_displayed_on**: On 'ModuleDisplayed', the page on which the module was displayed (e.g. 'artist').

**native_event_flattened_map_type**: Firebase event parameter `mapType`: the type of map the event refers to, as sent by the app.
