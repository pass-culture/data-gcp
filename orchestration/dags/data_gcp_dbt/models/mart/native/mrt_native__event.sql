{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
) }}

SELECT
    e.event_date,
    CASE WHEN e.event_name = "screen_view" THEN CONCAT(e.event_name, "_", e.firebase_screen ) ELSE e.event_name END AS event_name,
    e.event_timestamp,
    e.user_id,
    e.user_pseudo_id,
    e.origin,
    e.platform,
    e.app_version,
    e.traffic_campaign,
    e.traffic_source,
    e.traffic_medium,
    e.offer_id,
    e.session_id,
    e.unique_session_id,
    e.user_location_type,
    e.query,
    e.venue_id,
    e.booking_id,
    e.booking_cancellation_step,
    e.search_id,
    e.module_name,
    e.module_id,
    e.entry_id,
    e.onboarding_user_selected_age,
    o.offer_name,
    o.offer_category_id,
    o.offer_subcategory_id,
    v.venue_name,
    v.venue_type_label,
    c.content_type,
    d.deposit_type AS user_current_deposit_type,
    u.user_last_deposit_amount,
    u.user_first_deposit_type,
    u.user_deposit_initial_amount
FROM {{ ref('int_firebase__native_event') }} AS e
LEFT JOIN {{ ref('enriched_user_data') }} AS u ON e.user_id = u.user_id
LEFT JOIN {{ ref('mrt_global__offer') }} AS o ON e.offer_id = o.offer_id
LEFT JOIN {{ ref('mrt_global__venue') }} AS v ON v.venue_id = COALESCE(e.venue_id,o.venue_id)
LEFT JOIN {{ ref('int_contentful__entry') }} AS c ON c.id = e.module_id
LEFT JOIN {{ ref('int_applicative__deposit' ) }} AS d ON d.user_id = e.user_id AND e.event_date BETWEEN d.deposit_creation_date AND d.deposit_expiration_date
WHERE (
     event_name IN ("ConsultOffer",
      "BookingConfirmation",
      "StepperDisplayed",
      "ModuleDisplayedOnHomePage",
      "PlaylistHorizontalScroll",
      "ConsultVenue",
      "VenuePlaylistDisplayedOnSearchResults",
      "ClickBookOffer",
      "BookingConfirmation",
      "ContinueCGU",
      "HasAddedOfferToFavorites",
      "SelectAge",
      "Share",
      "CategoryBlockClicked",
      "HighlightBlockClicked",
      "ConsultVideo",
      "HasSeenAllVideo",
      "Screenshot",
      "NoSearchResult",
      "PerformSearch",
      "ConsultAvailableDates",
      "BookOfferConfirmDates",
      "ConsultVenueMap",
      "TrendsBlockClicked",
      "SystemBlockDisplayed",
      "ConsultHome")
    OR
    (
        e.event_name = "screen_view"
        AND e.firebase_screen IN  ("SignupForm","ProfilSignUp", "SignupConfirmationEmailSent", "OnboardingWelcome","OnboardingGeolocation", "FirstTutorial","BeneficiaryRequestSent","UnderageAccountCreated","BeneficiaryAccountCreated","FirstTutorial2","FirstTutorial3","FirstTutorial4","HasSkippedTutorial" )
    )
)
{% if is_incremental() %}
AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
{% endif %}
