SELECT
    event_date,
    event_name,
    timestamp(event_timestamp) as event_timestamp,
    user_id,
    user_pseudo_id,
    platform,
    app_version,
    STRUCT(
        traffic_campaign,
        traffic_source,
        traffic_medium,
        offer_id,
        unique_session_id,
        user_location_type,
        query,
        venue_id,
        booking_id,
        booking_cancellation_step,
        search_id,
        module_name,
        module_id,
        entry_id,
        onboarding_user_selected_age,
        offer_name,
        offer_category_id,
        offer_subcategoryId,
        venue_name,
        venue_type_label,
        content_type
    ) as extra_params,
    STRUCT(
        user_current_deposit_type,
        user_last_deposit_amount,
        user_first_deposit_type,
        user_deposit_initial_amount
    ) as user_params,
    "native" as origin
FROM {{ ref("mrt_native__event") }}
