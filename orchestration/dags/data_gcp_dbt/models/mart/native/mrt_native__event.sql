{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select
    e.event_date,
    case
        when e.event_name = "screen_view"
        then concat(e.event_name, "_", e.firebase_screen)
        else e.event_name
    end as event_name,
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
    o.partner_id,
    v.venue_name,
    v.venue_type_label,
    c.content_type,
    d.deposit_type as user_current_deposit_type,
    u.last_deposit_amount as user_last_deposit_amount,
    u.first_deposit_type as user_first_deposit_type,
    u.first_deposit_amount as user_first_deposit_amount
from {{ ref("int_firebase__native_event") }} as e
left join {{ ref("mrt_global__user") }} as u on e.user_id = u.user_id
left join {{ ref("mrt_global__offer") }} as o on e.offer_id = o.offer_id
left join
    {{ ref("mrt_global__venue") }} as v on v.venue_id = coalesce(e.venue_id, o.venue_id)
left join {{ ref("int_contentful__entry") }} as c on c.id = e.module_id
left join
    {{ ref("int_applicative__deposit") }} as d
    on d.user_id = e.user_id
    and e.event_date between d.deposit_creation_date and d.deposit_expiration_date
where
    (
        event_name in (
            "ConsultOffer",
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
            "ConsultHome",
            "ConsultArtist"
        )
        or (
            e.event_name = "screen_view"
            and e.firebase_screen in (
                "SignupForm",
                "ProfilSignUp",
                "SignupConfirmationEmailSent",
                "OnboardingWelcome",
                "OnboardingGeolocation",
                "FirstTutorial",
                "BeneficiaryRequestSent",
                "UnderageAccountCreated",
                "BeneficiaryAccountCreated",
                "FirstTutorial2",
                "FirstTutorial3",
                "FirstTutorial4",
                "HasSkippedTutorial",
                "AccountCreated"
            )
        )
    )
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    {% endif %}
