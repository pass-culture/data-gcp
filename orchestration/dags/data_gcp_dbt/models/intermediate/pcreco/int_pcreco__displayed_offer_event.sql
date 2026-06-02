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
        from {{ ref("int_firebase__native_event") }}
        where
            reco_call_id is not null
            and event_name in (
                "ConsultOffer",
                "BookingConfirmation",
                "HasAddedOfferToFavorites",
                "ModuleDisplayedOnHomePage",
                "PlaylistHorizontalScroll",
                "PlaylistVerticalScroll"
            )
            and (
                {% if is_incremental() %}
                    event_date
                    between date_sub(date("{{ ds() }}"), interval 3 day) and date(
                        "{{ ds() }}"
                    )
                {% else %}event_date >= date_sub(date("{{ ds() }}"), interval 60 day)
                {% endif %}
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
        from {{ ref("int_firebase__native_event") }}
        where
            reco_call_id is not null
            and event_name in ("ConsultOffer", "BookingConfirmation")
            and (
                {% if is_incremental() %}
                    event_date
                    between date_sub(date("{{ ds() }}"), interval 3 day) and date(
                        "{{ ds() }}"
                    )
                {% else %}event_date >= date_sub(date("{{ ds() }}"), interval 60 day)
                {% endif %}
            )
        group by reco_call_id, event_date, offer_id
    )

select
    et.* except (offer_context),
    d.total_module_consult_offer,
    d.total_module_booking_confirmation,
    d.total_module_add_to_favorites,
    struct(
        et.offer_context.offer_user_distance,
        et.offer_context.offer_is_geolocated,
        et.offer_context.offer_stock_price,
        datetime(et.offer_context.offer_creation_date) as offer_creation_date,
        datetime(
            et.offer_context.offer_stock_beginning_date
        ) as offer_stock_beginning_date,
        et.offer_context.offer_category,
        et.offer_context.offer_subcategory_id,
        et.offer_context.offer_booking_number,
        et.offer_context.offer_item_score,
        et.offer_context.offer_item_rank,
        et.offer_context.offer_ranking_origin,
        et.offer_context.offer_ranking_score,
        et.offer_context.offer_booking_number_last_7_days,
        et.offer_context.offer_booking_number_last_14_days,
        et.offer_context.offer_booking_number_last_28_days,
        et.offer_context.offer_semantic_emb_mean
    ) as offer_context,
    coalesce(i.is_consult_offer, 0) as is_consult_offer,
    coalesce(i.is_booking_confirmation, 0) as is_booking_confirmation,
    coalesce(i.is_add_to_favorites, 0) as is_add_to_favorites
from {{ ref("int_pcreco__past_offer_context_sink") }} as et
inner join
    displayed as d on et.event_date = d.event_date and et.reco_call_id = d.reco_call_id
left join
    interaction as i
    on et.event_date = i.event_date
    and et.reco_call_id = i.reco_call_id
    and et.offer_id = i.offer_id
{% if is_incremental() %}
    where
        et.event_date
        between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
{% endif %}
