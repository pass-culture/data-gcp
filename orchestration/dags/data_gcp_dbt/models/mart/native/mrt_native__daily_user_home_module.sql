{{ config(materialized="view") }}

select
    user_id,
    unique_session_id,
    displayed.entry_id,
    entry_name,
    module_id,
    module_name,
    typeform_id,
    parent_module_id,
    parent_modules.content_type as parent_module_type,
    parent_entry_id,
    parent_homes.content_type as parent_home_type,
    module_type,
    user_location_type,
    reco_call_id,
    displayed.app_version,
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
from {{ ref("int_firebase__native_daily_user_home_module") }} as displayed
left join
    {{ ref("int_contentful__entry") }} as parent_modules
    on parent_modules.id = displayed.parent_module_id
left join
    {{ ref("int_contentful__entry") }} as parent_homes
    on parent_homes.id = displayed.parent_entry_id
left join
    {{ ref("int_contentful__home_tag") }} as home_tag
    on home_tag.entry_id = displayed.entry_id
left join
    {{ ref("int_contentful__playlist_tag") }} as playlist_tag
    on playlist_tag.entry_id = displayed.module_id
