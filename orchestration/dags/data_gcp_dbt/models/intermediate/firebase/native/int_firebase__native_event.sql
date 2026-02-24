{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by="event_name",
            require_partition_filter=true,
        )
    )
}}

select
    event_date,
    event_timestamp,
    event_previous_timestamp,
    event_timestamp as user_first_touch_timestamp,
    user_id,
    user_pseudo_id,
    event_name,
    moduleid as module_id,
    platform,
    name,
    medium,
    source,
    app_version,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    ga_session_id as session_id,
    ga_session_number as session_number,
    shouldusealgoliarecommend as is_algolia_recommend,
    searchisautocomplete as search_is_autocomplete,
    searchisbasedonhistory as search_is_based_on_history,
    searchofferisduo as search_offer_is_duo_filter,
    geolocated as reco_geo_located,
    enabled,
    firebase_screen,
    firebase_previous_screen,
    pagename as page_name,
    origin,
    locationtype as user_location_type,
    categoryname as category_name,
    type as filter_type,
    venueid as venue_id,
    bookingid as booking_id,
    fromofferid as similar_offer_id,
    playlisttype as similar_offer_playlist_type,
    step as booking_cancellation_step,
    filtertypes as search_filter_types,
    searchid as search_id,
    filter,
    searchlocationfilter as search_location_filter,
    searchcategories as search_categories_filter,
    searchdate as search_date_filter,
    searchgenretypes as search_genre_types_filter,
    searchmaxprice as search_max_price_filter,
    searchnativecategories as search_native_categories_filter,
    modulename as module_name,
    modulelistid as module_list_id,
    index as module_index,
    items_0 as item_index_list,
    itemtype as item_type,
    displayed_offers,
    displayed_venues,
    traffic_gen,
    traffic_content,
    toentryid as destination_entry_id,
    recoorigin as reco_origin,
    abtest as reco_ab_test,
    callid as reco_call_id,
    modelversion as reco_model_version,
    modelname as reco_model_name,
    modelendpoint as reco_model_endpoint,
    social as selected_social_media,
    searchview as search_type,
    type as share_type,
    duration,
    appsflyeruserid as appsflyer_id,
    accessibilityfilter as search_accessibility_filter,
    videoduration as video_duration_seconds,
    seenduration as video_seen_duration_seconds,
    youtubeid as video_id,
    frommultivenueofferid as multi_venue_offer_id,
    artistid as artist_id,
    theme_setting,
    system_theme,
    coalesce(cast(double_offer_id as string), offerid) as offer_id,
    concat(user_pseudo_id, "-", ga_session_id) as unique_session_id,
    coalesce(query, searchquery) as query,
    concat(user_pseudo_id, "-", ga_session_id, "-", searchid) as unique_search_id,
    coalesce(entryid, homeentryid) as entry_id,
    case
        when entryid in ("4XbgmX7fVVgBMoCJiLiY9n", "1ZmUjN7Za1HfxlbAOJpik2")
        then "generale"
        when entryid is null
        then null
        else "marketing"
    end as home_type,
    coalesce(age, userstatus) as onboarding_user_selected_age,
    coalesce(isheadline = "true", false) as is_headline_offer,
    case when event_name = "ConsultOffer" then 1 else 0 end as is_consult_offer,
    case
        when event_name = "BookingConfirmation" then 1 else 0
    end as is_booking_confirmation,
    case
        when event_name = "HasAddedOfferToFavorites" then 1 else 0
    end as is_add_to_favorites,
    case when event_name = "Share" then 1 else 0 end as is_share,
    case when event_name = "Screenshot" then 1 else 0 end as is_screenshot,
    case when event_name = "UserSetLocation" then 1 else 0 end as is_set_location,
    case when event_name = "ConsultVideo" then 1 else 0 end as is_consult_video,
    case when event_name = "ConsultVenue" then 1 else 0 end as is_consult_venue,
    case when event_name = "screen_view" then 1 else 0 end as is_screen_view,
    case
        when event_name = "screen_view" and firebase_screen = "Home" then 1 else 0
    end as is_screen_view_home,
    case
        when event_name = "screen_view" and firebase_screen = "Search" then 1 else 0
    end as is_screen_view_search,
    case
        when event_name = "screen_view" and firebase_screen = "Offer" then 1 else 0
    end as is_screen_view_offer,
    case
        when event_name = "screen_view" and firebase_screen = "Profile" then 1 else 0
    end as is_screen_view_profile,
    case
        when event_name = "screen_view" and firebase_screen = "Favorites" then 1 else 0
    end as is_screen_view_favorites,
    case
        when
            event_name = "screen_view"
            and firebase_screen in ("Bookings", "BookingDetails")
        then 1
        else 0
    end as is_screen_view_bookings,
    case
        when
            firebase_screen = "SignupConfirmationEmailSent"
            or event_name = "ContinueCGU"
        then 1
        else 0
    end as is_signup_completed,
    case
        when
            firebase_screen in (
                "BeneficiaryRequestSent",
                "UnderageAccountCreated",
                "BeneficiaryAccountCreated"
            )
        then 1
        else 0
    end as is_benef_request_sent,
    case when event_name = "login" then 1 else 0 end as is_login,
    case
        when
            exists (
                select 1
                from {{ source("raw", "subcategories") }} as sc
                where
                    lower(query) like concat("%", lower(sc.category_id), "%")
                    or lower(query) like concat("%", lower(sc.id), "%")
            )
            or exists (
                select 1
                from {{ source("seed", "macro_rayons") }} as mr
                where
                    lower(query) like concat("%", lower(mr.macro_rayon), "%")
                    or lower(query) like concat("%", lower(mr.rayon), "%")
            )
        then true
        else false
    end as search_query_input_is_generic
from {{ ref("int_firebase__native_event_flattened") }}
where
    not is_anomaly
    {% if target.profile_name != "CI" %} and {% endif %}
    {% if is_incremental() %}
        event_date between date_sub(
            date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
        ) and date("{{ ds() }}")
    {% endif %}
