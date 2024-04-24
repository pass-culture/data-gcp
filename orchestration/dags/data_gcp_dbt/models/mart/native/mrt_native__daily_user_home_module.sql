{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "module_displayed_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

WITH redirections AS (
SELECT
    destination_entry_id AS entry_id,
    module_id AS parent_module_id,
    entry_id AS parent_entry_id,
    unique_session_id
FROM {{ref('int_firebase__native_event')}}
WHERE event_name IN ('CategoryBlockClicked','HighlightBlockClicked')
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
),

displayed AS (
SELECT
    events.unique_session_id,
    events.user_id,
    events.app_version,
    events.event_date AS module_displayed_date,
    events.event_timestamp AS module_displayed_timestamp,
    events.module_id,
    COALESCE(modules.title, modules.offer_title) AS module_name,
    modules.content_type AS module_type,
    events.entry_id AS entry_id,
    homes.title AS entry_name,
    redirections.parent_module_id,
    redirections.parent_entry_id,
    events.user_location_type,
    CASE WHEN modules.content_type = 'recommendation' THEN events.reco_call_id ELSE NULL END AS reco_call_id
FROM {{ref('int_firebase__native_event')}} AS events
LEFT JOIN redirections
    ON redirections.unique_session_id = events.unique_session_id AND redirections.entry_id = events.entry_id
INNER JOIN {{ ref('int_contentful__entry' )}} AS modules
    ON modules.id = events.module_id
INNER JOIN {{ ref('int_contentful__entry' )}} AS homes
    ON homes.id = events.entry_id
WHERE event_name = 'ModuleDisplayedOnHomePage'
    AND events.unique_session_id IS NOT NULL
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY events.unique_session_id, events.module_id ORDER BY event_timestamp ) = 1
),

clicked AS (
SELECT
    unique_session_id,
    entry_id,
    event_name AS click_type,
    module_id,
    module_list_id,
    event_timestamp AS module_clicked_timestamp
FROM {{ref('int_firebase__native_event')}}
WHERE unique_session_id IS NOT NULL
    AND event_name IN ("ExclusivityBlockClicked",
                        "CategoryBlockClicked",
                        "HighlightBlockClicked",
                        "BusinessBlockClicked",
                        "ConsultVideo")
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, module_id ORDER BY event_timestamp) = 1
),

consultations_offer AS (
SELECT
    unique_session_id,
    entry_id,
    module_id,
    origin,
    offer_id,
    venue_id,
    event_timestamp AS consult_offer_timestamp,
    user_location_type
FROM {{ref('int_firebase__native_event')}}
WHERE event_name = 'ConsultOffer'
    AND origin IN ("home",
                "exclusivity",
                "venue",
                "video",
                "videoModal",
                "highlightOffer")
    AND unique_session_id IS NOT NULL
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, offer_id ORDER BY event_timestamp DESC) = 1
),

consultations_venue AS (
SELECT unique_session_id,
    entry_id,
    module_id,
    offer_id,
    venue_id,
    event_timestamp AS consult_venue_timestamp,
    user_location_type
FROM {{ref('int_firebase__native_event')}}
WHERE event_name = "ConsultVenue"
    AND origin = "home"
    AND unique_session_id IS NOT NULL
    {% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, venue_id ORDER BY event_timestamp DESC) = 1
),

consultations AS (
SELECT o.unique_session_id,
    COALESCE(o.entry_id, v.entry_id) AS entry_id,
    COALESCE(o.module_id, v.module_id) AS module_id,
    o.origin,
    COALESCE(o.user_location_type, v.user_location_type) AS user_location_type,
    o.offer_id,
    v.venue_id,
    consult_offer_timestamp,
    consult_venue_timestamp
FROM consultations_venue AS v
FULL OUTER JOIN consultations_offer AS o
    ON o.unique_session_id = v.unique_session_id
        AND o.venue_id = v.venue_id
        AND o.consult_offer_timestamp >= v.consult_venue_timestamp
),

favorites AS (
SELECT consultations.unique_session_id,
    consultations.module_id,
    consultations.entry_id,
    consultations.offer_id,
    events.event_timestamp AS fav_timestamp
FROM {{ref('int_firebase__native_event')}} AS events
INNER JOIN consultations
USING (unique_session_id,
        module_id,
        entry_id,
        offer_id)
WHERE events.event_name = 'HasAddedOfferToFavorites'
    AND events.origin IN ("home",
    "exclusivity",
    "venue",
    "video",
    "videoModal",
    "highlightOffer")
AND events.unique_session_id IS NOT NULL
{% if is_incremental() %}
AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
{% endif %}
AND consult_offer_timestamp <= events.event_timestamp QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_session_id, offer_id ORDER BY event_timestamp DESC) = 1
)

SELECT
    displayed.user_id,
    displayed.unique_session_id,
    displayed.entry_id,
    displayed.entry_name,
    displayed.module_id,
    displayed.module_name,
    displayed.parent_module_id,
    parent_modules.content_type AS parent_module_type,
    displayed.parent_entry_id,
    parent_homes.content_type AS parent_home_type,
    displayed.module_type,
    displayed.user_location_type,
    displayed.reco_call_id,
    click_type,
    consultations.offer_id,
    consultations.venue_id,
    bookings.booking_id,
    module_displayed_date,
    module_displayed_timestamp,
    module_clicked_timestamp,
    consult_venue_timestamp,
    consult_offer_timestamp,
    fav_timestamp,
    bookings.booking_timestamp,
    home_tag.home_audience,
    home_tag.user_lifecycle_home,
    home_tag.home_type,
    playlist_tag.playlist_type,
    playlist_tag.offer_category,
    playlist_tag.playlist_reach,
    playlist_tag.playlist_recurrence
FROM displayed
LEFT JOIN clicked
    ON displayed.unique_session_id = clicked.unique_session_id
    AND displayed.entry_id = clicked.entry_id
    AND (displayed.module_id = clicked.module_id OR displayed.module_id = clicked.module_list_id)
    AND displayed.module_displayed_timestamp <= clicked.module_clicked_timestamp
LEFT JOIN consultations
    ON displayed.unique_session_id = consultations.unique_session_id
    AND consultations.module_id = displayed.module_id
    AND consultations.consult_offer_timestamp >= module_displayed_timestamp
LEFT JOIN favorites
    ON favorites.unique_session_id = displayed.unique_session_id
    AND favorites.module_id = displayed.module_id
LEFT JOIN {{ ref( 'firebase_bookings' ) }} AS bookings
    ON displayed.unique_session_id = bookings.unique_session_id
    AND consultations.offer_id = bookings.offer_id
    AND consultations.consult_offer_timestamp <= bookings.booking_timestamp
LEFT JOIN {{ ref('int_contentful__entry' )}} parent_modules
    ON parent_modules.id = displayed.parent_module_id
LEFT JOIN {{ ref('int_contentful__entry' )}} parent_homes
    ON parent_modules.id = displayed.parent_entry_id
LEFT JOIN {{ ref('int_contentful__home_tag' )}} home_tag
    ON home_tag.entry_id = displayed.entry_id
LEFT JOIN {{ ref('int_contentful__playlist_tag' )}} playlist_tag
    ON playlist_tag.entry_id = displayed.module_id