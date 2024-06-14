{{
    config(
        materialized = "view"
    )
}}

SELECT
    user_id,
    unique_session_id,
    displayed.entry_id,
    entry_name,
    module_id,
    module_name,
    parent_module_id,
    parent_modules.content_type AS parent_module_type,
    parent_entry_id,
    parent_homes.content_type AS parent_home_type,
    module_type,
    user_location_type,
    reco_call_id,
    click_type,
    displayed.offer_id,
    venue_id,
    booking_id,
    module_displayed_date,
    module_displayed_timestamp,
    module_clicked_timestamp,
    consult_venue_timestamp,
    consult_offer_timestamp,
    fav_timestamp,
    booking_timestamp,
    home_tag.home_audience,
    home_tag.user_lifecycle_home,
    home_tag.home_type,
    playlist_tag.playlist_type,
    playlist_tag.offer_category,
    playlist_tag.playlist_reach,
    playlist_tag.playlist_recurrence
FROM {{ ref('int_firebase__native_daily_user_home_module' )}} AS displayed
LEFT JOIN {{ ref('int_contentful__entry' )}} AS parent_modules
    ON parent_modules.id = displayed.parent_module_id
LEFT JOIN {{ ref('int_contentful__entry' )}} AS parent_homes
    ON parent_homes.id = displayed.parent_entry_id
LEFT JOIN {{ ref('int_contentful__home_tag' )}} AS home_tag
    ON home_tag.entry_id = displayed.entry_id
LEFT JOIN {{ ref('int_contentful__playlist_tag' )}} AS playlist_tag
    ON playlist_tag.entry_id = displayed.module_id
