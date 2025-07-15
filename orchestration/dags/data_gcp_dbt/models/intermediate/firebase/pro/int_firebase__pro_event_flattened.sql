select
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    event_date,
    device.category,
    device.operating_system,
    device.operating_system_version,
    device.web_info.browser,
    device.web_info.browser_version,
    timestamp_micros(event_timestamp) as event_timestamp,
    {{
        extract_params_int_value(
            [
                "ga_session_number",
                "ga_session_id",
                "offerer_id",
                "offererId",
                "page_number",
                "offerId",
                "categorieJuridiqueUniteLegale",
                "venueId",
                "filesCount",
            ]
        )
    }},
    {{
        extract_params_string_value(
            [
                "venue_id",
                "page_title",
                "page_location",
                "page_referrer",
                "offerType",
                "saved",
                "hasOnly6eAnd5eStudents",
                "isEdition",
                "isDraft",
                "filled",
                "filledWithErrors",
                "traffic_campaign",
                "traffic_medium",
                "traffic_source",
                "format",
                "bookingStatus",
                "buttonType",
                "fileType",
                "subcategoryId",
                "choosenSuggestedSubcategory",
                "status",
                "selected_offers",
                "imageCreationStage",
                "actionType",
            ]
        )
    }},
    (
        select event_params.value.double_value
        from unnest(event_params) as event_params
        where event_params.key = 'offerId'
    ) as double_offer_id,
    (
        select event_params.value.string_value
        from unnest(event_params) as event_params
        where event_params.key = 'from'
    ) as origin,
    (
        select event_params.value.string_value
        from unnest(event_params) as event_params
        where event_params.key = 'to'
    ) as destination
from {{ source("raw", "firebase_pro_events") }}
