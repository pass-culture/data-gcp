
{{ config(
    partition_by={
        'field': 'module_displayed_date',
        'data_type': 'date',
        'granularity': 'day'
    }
) }}

select
    uh.module_displayed_date,
    uh.module_id,
    uh.module_name,
    uh.module_type,
    uh.entry_id,
    uh.entry_name,
    uh.parent_module_id,
    uh.parent_module_type,
    uh.parent_entry_id,
    uh.app_version,
    uh.parent_home_type,
    uh.home_audience,
    uh.user_lifecycle_home,
    uh.home_type,
    uh.playlist_type,
    uh.offer_category,
    uh.playlist_reach,
    uh.playlist_recurrence,
    COALESCE(user_role, 'Grand Public') as user_role,
    COUNT(distinct unique_session_id) as total_session_display,
    COUNT(distinct case when consult_offer_timestamp is not NULL or click_type is not NULL or consult_venue_timestamp is not NULL then uh.unique_session_id end) as total_session_with_click,
    COUNT(distinct case when consult_offer_timestamp is not NULL then uh.unique_session_id end) as total_sesh_consult_offer,
    COUNT(distinct case when fav_timestamp is not NULL then uh.unique_session_id end) as total_session_fav,
    COUNT(distinct case when click_type = 'ConsultVideo' then uh.unique_session_id end) as total_session_with_consult_video,
    COUNT(case when consult_offer_timestamp is not NULL then 1 end) as total_consult_offer,
    COUNT(case when fav_timestamp is not NULL then 1 end) as total_fav,
    COUNT(distinct case when booking_timestamp is not NULL then uh.unique_session_id end) as total_session_with_booking,
    COUNT(case when booking_timestamp is not NULL then 1 end) as total_bookings,
    COUNT(case when uh.booking_id is not NULL then 1 end) as total_non_cancelled_bookings,
    SUM(db.delta_diversification) as total_diversification
from {{ ref( 'mrt_native__daily_user_home_module' ) }} as uh
    left join {{ ref('int_applicative__user') }} as u
        on u.user_id = uh.user_id
    left join {{ ref('diversification_booking') }} as db
        on db.booking_id = uh.booking_id
where module_displayed_date >= DATE_SUB(DATE('{{ ds() }}'), interval 6 month)

group by
    module_displayed_date,
    module_id,
    module_name,
    module_type,
    entry_id,
    entry_name,
    parent_module_id,
    parent_module_type,
    parent_entry_id,
    app_version,
    parent_home_type,
    home_audience,
    user_lifecycle_home,
    home_type,
    playlist_type,
    offer_category,
    playlist_reach,
    playlist_recurrence,
    user_role
