{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
        )
    )
}}

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
    int_applicative__offer_item_id.item_id,
    similar_offer_id,
    similar_int_applicative__offer_item_id.item_id as similar_item_id,
    booking_id,
    event_name,
    f_events.user_location_type,
    case
        when event_name = 'PlaylistVerticalScroll' then 'display' else 'convert'
    end as event_type
from {{ ref("int_firebase__native_event") }} as f_events
inner join
    {{ ref("int_applicative__offer_item_id") }} as int_applicative__offer_item_id
    using (offer_id)
left join
    {{ ref("int_applicative__offer_item_id") }}
    as similar_int_applicative__offer_item_id
    on f_events.similar_offer_id = similar_int_applicative__offer_item_id.offer_id
where
    (
        event_name = 'PlaylistVerticalScroll'
        or (event_name = 'ConsultOffer' and similar_offer_id is not null)
        or (event_name = 'BookingConfirmation' and similar_offer_id is not null)
    )
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
