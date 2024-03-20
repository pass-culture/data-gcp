SELECT  
    event_date, 
    event_name,
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_params,
    STRUCT(
        offer_name,
        offer_category_id,
        offer_subcategoryId,
        venue.venue_name,
        venue_type_label,
        content_type
    ) as extra_params,
    STRUCT(
        user_current_deposit_type,
        user_last_deposit_amount,
        user_first_deposit_type,
        user_deposit_initial_amount
    ) as user_params,
    platform,
    app_info.version as app_version,
    'native' as origin
FROM `{{ bigquery_raw_dataset }}.firebase_events`
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` USING(user_id)
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` offer
ON offer.offer_id = (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'offerId'
        )
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_venue_data` venue
ON venue.venue_id = COALESCE((
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'venueId'
        ), offer.venue_id)
LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` contentful
ON contentful.id = (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'moduleId'
        )
WHERE event_date = DATE('{{ add_days(ds, params.days) }}')
    AND (
       event_name IN ('ConsultOffer',
      'BookingConfirmation',
      'StepperDisplayed',
      'ModuleDisplayedOnHomePage',
      'PlaylistHorinzontalScroll',
      'ConsultVenue',
      'VenuePlaylistDisplayedOnSearchResults',
      'ClickBookOffer',
      'BookingConfirmation',
      'ContinueCGU',
      'HasAddedOfferToFavorites',
      'SelectAge',
      'Share',
      'CategoryBlockClicked',
      'HighlightBlockClicked',
      'ConsultVideo',
      'HasSeenAllVideo',
      'Screenshot',
      'NoSearchResult',
      'PerformSearch')
    OR
    (
        event_name = "screen_view" 
        AND (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'firebase_screen'
        ) IN ('SignupForm','ProfilSignUp', 'SignupConfirmationEmailSent', 'OnboardingWelcome','OnboardingGeolocation', 'FirstTutorial','BeneficiaryRequestSent','UnderageAccountCreated','BeneficiaryAccountCreated','FirstTutorial2','FirstTutorial3','FirstTutorial4','HasSkippedTutorial' )
    )
)
AND  MOD(ABS(FARM_FINGERPRINT(user_pseudo_id)),10) = 0
