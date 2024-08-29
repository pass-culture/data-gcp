{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'event_date', 'data_type': 'date'},
    )
) }}

select
    user_id,
    session_id,
    unique_session_id,
    event_date,
    event_timestamp,
    app_version,
    similar_offer_playlist_type,
    is_algolia_recommend,
    reco_call_id,
    f_events.offer_id,
    offer_item_ids.item_id,
    similar_offer_id,
    similar_offer_item_ids.item_id as similar_item_id,
    booking_id,
    event_name,
    f_events.user_location_type,
    case when event_name = 'PlaylistVerticalScroll' then 'display' else 'convert' end as event_type
from {{ ref('int_firebase__native_event') }} f_events
    inner join {{ ref('offer_item_ids') }} offer_item_ids using (offer_id)
    left join {{ ref('offer_item_ids') }} similar_offer_item_ids on similar_offer_item_ids.offer_id = f_events.similar_offer_id
where (
    event_name = 'PlaylistVerticalScroll'
    or (event_name = 'ConsultOffer' and similar_offer_id is not NULL)
    or (event_name = 'BookingConfirmation' and similar_offer_id is not NULL)
)
{% if is_incremental() %}
    and event_date between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
{% endif %}
