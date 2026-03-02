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
            require_partition_filter=True,
        )
    )
}}

with
    export_table as (
        select
            cast(reco_sink.jsonpayload.extra.date as timestamp) as event_created_at,
            reco_sink.jsonpayload.extra.call_id as reco_call_id,
            reco_sink.jsonpayload.extra.context,
            cast(reco_sink.jsonpayload.extra.user_id as string) as user_id,
            cast(reco_sink.jsonpayload.extra.offer_id as string) as offer_id,
            int_applicative__offer_item_id.item_id,
            reco_sink.jsonpayload.extra.context_extra_data.offer_origin_ids
            as offer_origin_id,
            reco_sink.jsonpayload.extra.context_extra_data.model_params.name
            as model_params_name,
            reco_sink.jsonpayload.extra.context_extra_data.model_params.description
            as model_params_description,
            reco_sink.jsonpayload.extra.context_extra_data.scorer.ranking.model_display_name
            as scorer_ranking_model_display_name,
            reco_sink.jsonpayload.extra.context_extra_data.scorer.ranking.model_version
            as scorer_ranking_model_version,
            date(reco_sink.jsonpayload.extra.date) as event_date,
            case
                when reco_sink.jsonpayload.extra.context like "similar_offer:%"
                then "similar_offer"
                when
                    reco_sink.jsonpayload.extra.context like "recommendation_fallback:%"
                then "similar_offer"
                when reco_sink.jsonpayload.extra.context like "recommendation:%"
                then "recommendation"
                else "unknown"
            end as playlist_origin,
            round(reco_sink.jsonpayload.extra.offer_order) as offer_display_order,
            struct(
                reco_sink.jsonpayload.extra.user_deposit_remaining_credit,
                reco_sink.jsonpayload.extra.user_bookings_count,
                reco_sink.jsonpayload.extra.user_clicks_count,
                reco_sink.jsonpayload.extra.user_favorites_count,
                reco_sink.jsonpayload.extra.user_iris_id,
                ii.centroid as user_iris_centroid,
                reco_sink.jsonpayload.extra.user_is_geolocated
            ) as user_context,
            struct(
                reco_sink.jsonpayload.extra.offer_user_distance,
                reco_sink.jsonpayload.extra.offer_is_geolocated,
                reco_sink.jsonpayload.extra.offer_stock_price,
                date(
                    cast(reco_sink.jsonpayload.extra.offer_creation_date as timestamp)
                ) as offer_creation_date,
                date(
                    cast(
                        reco_sink.jsonpayload.extra.offer_stock_beginning_date
                        as timestamp
                    )
                ) as offer_stock_beginning_date,
                reco_sink.jsonpayload.extra.offer_category,
                reco_sink.jsonpayload.extra.offer_subcategory_id,
                reco_sink.jsonpayload.extra.offer_booking_number,
                reco_sink.jsonpayload.extra.offer_item_score,
                reco_sink.jsonpayload.extra.offer_item_rank,
                reco_sink.jsonpayload.extra.offer_extra_data.offer_ranking_origin
                as offer_ranking_origin,
                cast(
                    reco_sink.jsonpayload.extra.offer_extra_data.offer_ranking_score
                    as float64
                ) as offer_ranking_score,
                cast(
                    reco_sink.jsonpayload.extra.offer_extra_data.offer_booking_number_last_7_days
                    as float64
                ) as offer_booking_number_last_7_days,
                cast(
                    reco_sink.jsonpayload.extra.offer_extra_data.offer_booking_number_last_14_days
                    as float64
                ) as offer_booking_number_last_14_days,
                cast(
                    reco_sink.jsonpayload.extra.offer_extra_data.offer_booking_number_last_28_days
                    as float64
                ) as offer_booking_number_last_28_days,
                cast(
                    reco_sink.jsonpayload.extra.offer_extra_data.offer_semantic_emb_mean
                    as float64
                ) as offer_semantic_emb_mean
            ) as offer_context,
            reco_sink.jsonpayload.extra.context_extra_data.scorer.retrievals[
                safe_offset(0)
            ].model_display_name as scorer_retrieval_model_display_name,
            reco_sink.jsonpayload.extra.context_extra_data.scorer.retrievals[
                safe_offset(0)
            ].model_version as scorer_retrieval_model_version
        from {{ source("raw", "run_googleapis_com_stderr") }} as reco_sink
        inner join
            {{ ref("int_applicative__offer_item_id") }}
            as int_applicative__offer_item_id
            on reco_sink.jsonpayload.extra.offer_id
            = int_applicative__offer_item_id.offer_id
        left join
            {{ ref("int_seed__iris_france") }} as ii
            on reco_sink.jsonpayload.extra.user_iris_id = ii.id
        where
            reco_sink.resource.type = "cloud_run_revision"
            and reco_sink.jsonpayload.extra.labels.event_type
            = "recommendation_past_offer_context_sink"

            and (
                {% if is_incremental() %}
                    date(reco_sink.jsonpayload.extra.date) between date_sub(
                        date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
                    ) and date("{{ ds() }}")
                {% else %}
                    date(reco_sink.jsonpayload.extra.date)
                    >= date_sub(date("{{ ds() }}"), interval 60 day)
                {% endif %}
            )

        qualify
            row_number() over (
                partition by
                    reco_sink.jsonpayload.extra.user_id,
                    reco_sink.jsonpayload.extra.call_id,
                    reco_sink.jsonpayload.extra.offer_id
                order by reco_sink.jsonpayload.extra.date desc
            )
            = 1
    )

select et.*
from export_table as et
{% if is_incremental() %}
    where
        et.event_date between date_sub(
            date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
        ) and date("{{ ds() }}")
{% else %} where et.event_date >= date_sub(date("{{ ds() }}"), interval 60 day)
{% endif %}
