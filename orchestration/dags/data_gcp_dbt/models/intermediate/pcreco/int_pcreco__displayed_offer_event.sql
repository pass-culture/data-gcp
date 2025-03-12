{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by="playlist_origin",
        )
    )
}}

with
    displayed as (
        select
            reco_call_id,
            event_date,
            sum(is_consult_offer) as total_module_consult_offer,
            sum(is_booking_confirmation) as total_module_booking_confirmation,
            sum(is_add_to_favorites) as total_module_add_to_favorites
        from {{ ref("int_firebase__native_event") }} fsoe
        where
            {% if is_incremental() %}
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
                and
            {% endif %}
            reco_call_id is not null
            and event_name in (
                "ConsultOffer",
                "BookingConfirmation",
                "HasAddedOfferToFavorites",
                "ModuleDisplayedOnHomePage",
                "PlaylistHorizontalScroll",
                "PlaylistVerticalScroll"
            )
        group by reco_call_id, event_date
    ),

    interaction as (
        select
            reco_call_id,
            event_date,
            offer_id,
            max(is_consult_offer) as is_consult_offer,
            max(is_booking_confirmation) as is_booking_confirmation,
            max(is_add_to_favorites) as is_add_to_favorites
        from {{ ref("int_firebase__native_event") }} fsoe
        where
            {% if is_incremental() %}
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
                and
            {% endif %}
            reco_call_id is not null
            and event_name in ("ConsultOffer", "BookingConfirmation")
        group by reco_call_id, event_date, offer_id
    )

select
    et.*,
    d.total_module_consult_offer,
    d.total_module_booking_confirmation,
    d.total_module_add_to_favorites,
    coalesce(i.is_consult_offer, 0) as is_consult_offer,
    coalesce(i.is_booking_confirmation, 0) as is_booking_confirmation,
    coalesce(i.is_add_to_favorites, 0) as is_add_to_favorites
from {{ ref("int_pcreco__past_offer_context") }} et
inner join
    displayed d on d.event_date = et.event_date and d.reco_call_id = et.reco_call_id
left join
    interaction i
    on i.event_date = et.event_date
    and i.reco_call_id = et.reco_call_id
    and i.offer_id = et.offer_id
{% if is_incremental() %}
    where
        et.event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
{% endif %}
