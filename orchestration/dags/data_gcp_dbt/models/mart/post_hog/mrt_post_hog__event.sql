SELECT
    event_date,
    event_name,
    event_timestamp,
    user_id,
    user_pseudo_id,
    platform,
    app_version,
    STRUCT(
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
    'native' as origin
FROM {{ ref('mrt_global__firebase_event') }}
WHERE MOD(ABS(FARM_FINGERPRINT(user_pseudo_id)),10) = 0
