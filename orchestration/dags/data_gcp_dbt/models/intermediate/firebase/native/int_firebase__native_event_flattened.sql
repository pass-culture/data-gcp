{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
            require_partition_filter=true,
        )
    )
}}

with
    firebase_last_two_days_events as (
        select *
        from {{ source("raw", "firebase_events") }}
        where
            event_date > date("1970-01-01")
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 3 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
    ),

    native_unnest as (
        select
            event_date,
            user_id,
            user_pseudo_id,
            event_name,
            timestamp_micros(event_timestamp) as event_timestamp,
            timestamp_micros(event_previous_timestamp) as event_previous_timestamp,
            timestamp_micros(event_timestamp) as user_first_touch_timestamp,
            platform,
            traffic_source.name,
            traffic_source.medium,
            traffic_source.source,
            app_info.version as app_version,
            (
                select event_params.value.double_value
                from unnest(event_params) event_params
                where event_params.key = 'offerId'
            ) as double_offer_id,
            {{
                extract_params_int_value(
                    [
                        "ga_session_id",
                        "ga_session_number",
                        "shouldUseAlgoliaRecommend",
                        "searchIsAutocomplete",
                        "searchIsBasedOnHistory",
                        "searchOfferIsDuo",
                        "geo_located",
                        "enabled",
                    ]
                )
            }}
            {{
                extract_params_string_value(
                    [
                        "searchId",
                        "moduleId",
                        "moduleName",
                        "playlistType",
                        "entryId",
                        "homeEntryId",
                        "locationType",
                        "step",
                        "searchGenreTypes",
                        "call_id",
                        "searchLocationFilter",
                        "age",
                        "searchCategories",
                        "firebase_screen",
                        "firebase_previous_screen",
                        "fromMultivenueOfferId",
                        "pageName",
                        "offerId",
                        "query",
                        "searchQuery",
                        "categoryName",
                        "type",
                        "venueId",
                        "bookingId",
                        "fromOfferId",
                        "filterTypes",
                        "filter",
                        "searchDate",
                        "searchMaxPrice",
                        "searchNativeCategories",
                        "moduleListID",
                        "index",
                        "traffic_campaign",
                        "traffic_source",
                        "traffic_medium",
                        "traffic_gen",
                        "traffic_content",
                        "toEntryId",
                        "reco_origin",
                        "ab_test",
                        "model_version",
                        "model_name",
                        "model_endpoint",
                        "userStatus",
                        "social",
                        "searchView",
                        "duration",
                        "appsFlyerUserId",
                        "accessibilityFilter",
                        "offers_1_10",
                        "offers_11_20",
                        "offers_21_30",
                        "offers_31_40",
                        "offers_41_50",
                        "venues_1_10",
                        "venues_11_20",
                        "venues_21_30",
                        "venues_31_40",
                        "venues_41_50",
                        "videoDuration",
                        "seenDuration",
                        "youtubeId",
                    ]
                )
            }},
            (
                select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'from'
            ) as origin
        from firebase_last_two_days_events
    )

select
    * except (
        offers_1_10,
        offers_11_20,
        offers_21_30,
        offers_31_40,
        offers_41_50,
        venues_1_10,
        venues_11_20,
        venues_21_30,
        venues_31_40,
        venues_41_50
    ),
    {{ extract_str_to_array_field("offers", 0, 10, 50) }} as displayed_offers,
    {{ extract_str_to_array_field("venues", 0, 10, 50) }} as displayed_venues,
    case
        when event_name = 'BookingConfirmation' and bookingid is null
        then true
        when event_name = 'BookingConfirmation' and user_id is null
        then true
        when event_name = 'ConsultOffer' and offerid is null
        then true
        else false
    end as is_anomaly
from native_unnest
