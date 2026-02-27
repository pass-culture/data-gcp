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
            cluster_by="reco_call_id",
        )
    )
}}

with
    items_score as (
        select
            item_id, sum(coalesce(total_individual_bookings, 0)) as item_booking_count
        from {{ ref("mrt_global__offer") }}
        group by 1
    ),

    discovery_data as (
        select consultation_id, discovery_score
        from {{ ref("mrt_native__consultation") }}
        {% if is_incremental() %}
            where
                consultation_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %}
            where consultation_date >= date_sub(date('{{ ds() }}'), interval 6 day)
        {% endif %}
    ),

    diversity_data as (
        select
            div_score.booking_creation_date,
            div_score.booking_id,
            div_score.diversity_score
        from {{ ref("int_metric__diversity_score") }} as div_score
        {% if is_incremental() %}
            where
                div_score.booking_creation_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %}
            where
                div_score.booking_creation_date
                >= date_sub(date('{{ ds() }}'), interval 6 day)
        {% endif %}
    ),

    events as (
        select
            event_date,
            platform,
            unique_session_id,
            event_timestamp,
            user_id,
            event_name,
            reco_call_id,
            offer_id,
            similar_offer_id,
            booking_id,
            module_id,
            similar_offer_playlist_type,
            item_type,
            item_index_list
        from {{ ref("int_firebase__native_event") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %} where event_date >= date_sub(date('{{ ds() }}'), interval 6 day)
        {% endif %}
    ),

    past_offer_context as (
        select
            event_date,
            reco_call_id,
            playlist_origin,
            context,
            offer_context.offer_item_score,
            model_params_name,
            model_params_description,
            scorer_retrieval_model_display_name,
            scorer_ranking_model_display_name,
            offer_id,
            item_id,
            offer_display_order,
            offer_context.offer_category,
            offer_context.offer_booking_number
        from {{ ref("int_pcreco__past_offer_context_sink") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %} where event_date >= date_sub(date('{{ ds() }}'), interval 6 day)
        {% endif %}
    ),

    base_events as (
        select
            e.event_date,
            e.platform,
            e.unique_session_id,
            e.event_timestamp,
            e.user_id,
            e.event_name,
            e.reco_call_id,
            e.offer_id,
            e.booking_id,
            case
                when e.module_id is not null
                then e.module_id
                when e.event_name = 'PlaylistVerticalScroll'
                then concat(e.offer_id, '-', e.similar_offer_playlist_type)
                when e.event_name = 'ConsultOffer'
                then concat(e.similar_offer_id, '-', e.similar_offer_playlist_type)
            end as display_type,
            coalesce(e.module_id, e.similar_offer_playlist_type) as loose_display_type,
            split(item, ':')[offset(1)] as item_id
        from
            events as e,
            unnest(
                if(
                    e.event_name = 'ViewItem' and e.item_type = 'offer',
                    split(e.item_index_list, ','),
                    [cast(null as string)]
                )
            ) as item
        where
            e.unique_session_id is not null
            and (
                e.event_name in (
                    'ModuleDisplayedOnHomePage',
                    'PlaylistVerticalScroll',
                    'ViewItem',
                    'ConsultOffer',
                    'HasAddedOfferToFavorites',
                    'BookingConfirmation'
                )
            )
    ),

    reco_events_algos as (
        select
            e.event_date,
            e.platform,
            e.unique_session_id,
            e.event_timestamp,
            e.user_id,
            e.display_type,
            e.loose_display_type,
            algo.reco_call_id,
            algo.playlist_origin,
            algo.context,
            algo.model_params_name,
            algo.model_params_description,
            algo.scorer_retrieval_model_display_name,
            algo.scorer_ranking_model_display_name,
            algo.offer_id,
            algo.item_id,
            cast(algo.offer_display_order as int64) as offer_reco_rank,
            algo.offer_category,
            algo.offer_booking_number,
            i.item_booking_count,
            case
                when algo.offer_item_score > 1
                then 'Top Offer'
                when algo.offer_item_score <= 1
                then 'Two Tower'
                else 'Unknown'
            end as retrieval
        from past_offer_context as algo
        inner join base_events as e on algo.reco_call_id = e.reco_call_id
        inner join items_score as i on algo.item_id = i.item_id
        where
            e.reco_call_id is not null
            and e.event_name in ('ModuleDisplayedOnHomePage', 'PlaylistVerticalScroll')
    ),

    enriched_algos as (
        select
            algo.*,
            farm_fingerprint(
                concat(
                    algo.playlist_origin,
                    '|',
                    algo.context,
                    '|',
                    algo.retrieval,
                    '|',
                    algo.model_params_name,
                    '|',
                    algo.model_params_description,
                    '|',
                    algo.scorer_retrieval_model_display_name,
                    '|',
                    algo.scorer_ranking_model_display_name,
                    '|',
                    algo.offer_category
                )
            ) as reco_config_id
        from reco_events_algos as algo
    ),

    view_events as (
        select
            unique_session_id,
            item_id,
            loose_display_type,
            min(event_timestamp) as view_ts
        from base_events
        where event_name = 'ViewItem'
        group by 1, 2, 3
    ),

    bonus_conversion as (
        select base_events.*
        from base_events
        where
            base_events.event_name
            in ('HasAddedOfferToFavorites', 'BookingConfirmation')
    ),

    conversion_events as (
        select
            c.unique_session_id,
            c.offer_id,
            c.display_type,
            min(
                if(c.event_name = 'ConsultOffer', c.event_timestamp, null)
            ) as consult_ts,
            min(
                if(b.event_name = 'HasAddedOfferToFavorites', b.event_timestamp, null)
            ) as fav_ts,
            min(
                if(b.event_name = 'BookingConfirmation', b.event_timestamp, null)
            ) as book_ts,
            max(disco.discovery_score) as discovery_score,
            max(div.diversity_score) as diversity_score
        from
            (
                select base_events.*
                from base_events
                where base_events.event_name = 'ConsultOffer'
            ) as c
        left join
            bonus_conversion as b
            on c.unique_session_id = b.unique_session_id
            and c.offer_id = b.offer_id
        left join
            discovery_data as disco
            on disco.consultation_id
            = concat(c.user_id, '-', c.event_timestamp, '-', c.offer_id)
        left join
            diversity_data as div
            on b.event_name = 'BookingConfirmation'
            and b.booking_id = div.booking_id
        group by 1, 2, 3
        having c.consult_ts is not null
    ),

    conversion as (
        select
            e.*,
            if(v.view_ts is not null, 1, 0) as offer_viewed,
            if(e.event_timestamp <= c.consult_ts, 1, 0) as offer_consulted,
            if(
                c.fav_ts is not null
                and e.event_timestamp <= c.consult_ts
                and e.event_timestamp <= c.fav_ts,
                1,
                0
            ) as offer_faved,
            if(
                c.book_ts is not null
                and e.event_timestamp <= c.consult_ts
                and e.event_timestamp <= c.book_ts,
                1,
                0
            ) as offer_booked,
            if(
                e.event_timestamp <= c.consult_ts, c.discovery_score, 0
            ) as discovery_score,
            if(
                e.event_timestamp <= c.consult_ts and e.event_timestamp <= c.book_ts,
                c.diversity_score,
                0
            ) as diversity_score,
            if(
                e.event_timestamp <= c.consult_ts and e.event_timestamp <= c.book_ts,
                1 / e.item_booking_count,
                0
            ) as item_niche_score,
            if(
                e.event_timestamp <= c.consult_ts and e.event_timestamp <= c.book_ts,
                1 / (e.offer_booking_number + 1),
                0
            ) as offer_niche_score
        from enriched_algos as e
        left join
            view_events as v
            on e.unique_session_id = v.unique_session_id
            and e.offer_id = v.item_id
            and e.loose_display_type = v.loose_display_type
        left join
            conversion_events as c
            on e.unique_session_id = c.unique_session_id
            and e.offer_id = c.offer_id
            and e.display_type = c.display_type
    )

select distinct * except (display_type, loose_display_type)
from conversion
