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

with
    firebase_last_two_days_events as (
        select *
        from {{ source("raw", "firebase_events") }}
        where
            true
            {% if target.profile_name != "CI" %} and {% endif %}
            {% if is_incremental() %}
                event_date
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
            platform,
            traffic_source.name,
            traffic_source.medium,
            traffic_source.source,
            app_info.version as app_version,
            timestamp_micros(event_timestamp) as event_timestamp,
            timestamp_micros(event_previous_timestamp) as event_previous_timestamp,
            timestamp_micros(event_timestamp) as user_first_touch_timestamp,
            (
                select event_params.value.double_value
                from unnest(event_params) as event_params
                where event_params.key = "offerId"
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
            }},
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
                        "query",
                        "searchQuery",
                        "categoryName",
                        "type",
                        "venueId",
                        "fromOfferId",
                        "filterTypes",
                        "filter",
                        "searchDate",
                        "searchMaxPrice",
                        "searchNativeCategories",
                        "moduleListID",
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
                        "isHeadline",
                        "items_0",
                        "itemType",
                        "index",
                    ]
                )
            }},
            -- noqa: disable=CP02
            -- fmt: off
            coalesce(
                {{ extract_params_string_value(["bookingId"], alias=false) }},
                cast(
                    {{ extract_params_int_value(["bookingId"], alias=false) }} as string
                )
            ) as bookingId, -- dbt internal hook creates tmp table for incremental and compare fields names (including the capitalization), if not biquery (case insensitive) breaks
            coalesce(
                {{ extract_params_string_value(["offerId"], alias=false) }},
                cast({{ extract_params_int_value(["offerId"], alias=false) }} as string)
            ) as offerId, -- dbt internal hook creates tmp table for incremental and compare fields names (including the capitalization), if not biquery (case insensitive) breaks
            -- fmt: on
            coalesce(
                (
                    select event_params.value.string_value
                    from unnest(event_params) as event_params
                    where event_params.key = "from"
                ),
                (
                    select event_params.value.string_value
                    from unnest(event_params) as event_params
                    where event_params.key = "origin"
                )
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
        when event_name = "BookingConfirmation" and bookingid is null  -- this is not in the hook so case insensitive is fine
        then true
        when event_name = "BookingConfirmation" and user_id is null
        then true
        when event_name = "ConsultOffer" and offerid is null  -- this is not in the hook so case insensitive is fine
        then true
        else false
    end as is_anomaly
from native_unnest
