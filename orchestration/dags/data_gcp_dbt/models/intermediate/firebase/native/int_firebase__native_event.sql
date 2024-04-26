{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
        alias = "firebase_events",
        cluster_by = "event_name"
    )
}}

SELECT
    event_date,
    event_timestamp,
    event_previous_timestamp,
    event_timestamp AS user_first_touch_timestamp,
    user_id,
    user_pseudo_id,
    event_name,
    moduleId AS module_id,
    platform,
    name,
    medium,
    source,
    app_version,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    COALESCE(CAST(double_offer_id AS string),offerId) AS offer_id,
    ga_session_id AS session_id,
    CONCAT(user_pseudo_id, "-",ga_session_id) AS unique_session_id,
    ga_session_number AS session_number,
    shouldUseAlgoliaRecommend AS is_algolia_recommend,
    searchIsAutocomplete AS search_is_autocomplete,
    searchIsBasedOnHistory AS search_is_based_on_history,
    searchOfferIsDuo AS search_offer_is_duo_filter,
    geo_located AS reco_geo_located,
    enabled,
    firebase_screen,
    firebase_previous_screen,
    pageName AS page_name,
    origin,
    locationType AS user_location_type,
    COALESCE(query,searchQuery) AS query,
    categoryName AS category_name,
    type AS filter_type,
    venueId AS venue_id,
    bookingId AS booking_id,
    fromOfferId AS similar_offer_id,
    playlistType AS similar_offer_playlist_type,
    step AS booking_cancellation_step,
    filterTypes AS search_filter_types,
    searchId AS search_id,
    CONCAT(user_pseudo_id, "-",ga_session_id,"-",searchId) AS unique_search_id,
    filter,
    searchLocationFilter AS search_location_filter,
    searchCategories AS search_categories_filter,
    searchDate AS search_date_filter,
    searchGenreTypes AS search_genre_types_filter,
    searchMaxPrice AS search_max_price_filter,
    searchNativeCategories AS search_native_categories_filter,
    moduleName AS module_name,
    moduleListID AS module_list_id,
    index AS module_index,
    traffic_gen,
    traffic_content,
    COALESCE(entryId,homeEntryId) AS entry_id,
    CASE WHEN entryId IN ('4XbgmX7fVVgBMoCJiLiY9n','1ZmUjN7Za1HfxlbAOJpik2') THEN "generale"
        WHEN entryId IS NULL THEN NULL
        ELSE "marketing" END AS home_type,
    toEntryId AS destination_entry_id,
    reco_origin,
    ab_test AS reco_ab_test,
    call_id AS reco_call_id,
    model_version AS reco_model_version,
    model_name AS reco_model_name,
    model_endpoint AS reco_model_endpoint,
    COALESCE(age,userStatus) AS onboarding_user_selected_age,
    social AS selected_social_media,
    searchView AS search_type,
    type AS share_type,
    duration,
    appsFlyerUserId AS appsflyer_id,
    accessibilityFilter AS search_accessibility_filter,
    CASE WHEN event_name = "ConsultOffer" THEN 1 ELSE 0 END AS is_consult_offer,
    CASE WHEN event_name = "BookingConfirmation" THEN 1 ELSE 0 END AS is_booking_confirmation,
    CASE WHEN event_name = "HasAddedOfferToFavorites" THEN 1 ELSE 0 END AS is_add_to_favorites,
    CASE WHEN event_name = "Share" THEN 1 ELSE 0 END AS is_share,
    CASE WHEN event_name = "Screenshot" THEN 1 ELSE 0 END AS is_screenshot,
    CASE WHEN event_name = "UserSetLocation" THEN 1 ELSE 0 END AS is_set_location,
    CASE WHEN event_name = "ConsultVideo" THEN 1 ELSE 0 END AS is_consult_video,
    CASE WHEN event_name = "ConsultVenue" THEN 1 ELSE 0 END AS is_consult_venue,
    CASE WHEN event_name = "screen_view" THEN 1 ELSE 0 END AS is_screen_view,
    CASE WHEN event_name = "screen_view" AND firebase_screen = "Home" THEN 1 ELSE 0 END AS is_screen_view_home,
    CASE WHEN event_name = "screen_view" AND firebase_screen = "Search" THEN 1 ELSE 0 END AS is_screen_view_search,
    CASE WHEN event_name = "screen_view" AND firebase_screen = "Offer" THEN 1 ELSE 0 END AS is_screen_view_offer,
    CASE WHEN event_name = "screen_view" AND firebase_screen = "Profile" THEN 1 ELSE 0 END AS is_screen_view_profile,
    CASE WHEN event_name = "screen_view" AND firebase_screen = "Favorites" THEN 1 ELSE 0 END AS is_screen_view_favorites,
    CASE WHEN event_name = "screen_view" AND firebase_screen IN ("Bookings","BookingDetails") THEN 1 ELSE 0 END AS is_screen_view_bookings,
    CASE WHEN firebase_screen = "SignupConfirmationEmailSent" OR event_name = "ContinueCGU" THEN 1 ELSE 0 END AS is_signup_completed,
    CASE WHEN firebase_screen IN ("BeneficiaryRequestSent","UnderageAccountCreated","BeneficiaryAccountCreated") THEN 1 ELSE 0 END AS is_benef_request_sent,
    CASE WHEN event_name = "login" THEN 1 ELSE 0 END AS is_login,
FROM {{ ref("int_firebase__native_event_flattened") }} AS e
{% if is_incremental() %}
WHERE event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
{% endif %}
