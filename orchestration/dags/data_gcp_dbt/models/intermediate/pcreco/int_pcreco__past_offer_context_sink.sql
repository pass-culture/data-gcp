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
            date(recommendation_sink.jsonpayload.extra.date) as event_date,
            cast(
                recommendation_sink.jsonpayload.extra.date as timestamp
            ) as event_created_at,
            jsonpayload.extra.call_id as reco_call_id,
            case
                when jsonpayload.extra.context like "similar_offer:%"
                then "similar_offer"
                when jsonpayload.extra.context like "recommendation_fallback:%"
                then "similar_offer"
                when jsonpayload.extra.context like "recommendation:%"
                then "recommendation"
                else "unknown"
            end as playlist_origin,
            jsonpayload.extra.context,
            round(jsonpayload.extra.offer_order) as offer_display_order,
            cast(jsonpayload.extra.user_id as string) as user_id,
            cast(jsonpayload.extra.offer_id as string) as offer_id,
            int_applicative__offer_item_id.item_id as item_id,
            struct(
                jsonpayload.extra.user_deposit_remaining_credit,
                jsonpayload.extra.user_bookings_count,
                jsonpayload.extra.user_clicks_count,
                jsonpayload.extra.user_favorites_count,
                jsonpayload.extra.user_iris_id,
                ii.centroid as user_iris_centroid,
                jsonpayload.extra.user_is_geolocated
            ) as user_context,
            struct(
                jsonpayload.extra.offer_user_distance,
                jsonpayload.extra.offer_is_geolocated,
                jsonpayload.extra.offer_stock_price,
                date(
                    cast(jsonpayload.extra.offer_creation_date as timestamp)
                ) as offer_creation_date,
                date(
                    cast(jsonpayload.extra.offer_stock_beginning_date as timestamp)
                ) as offer_stock_beginning_date,
                jsonpayload.extra.offer_category,
                jsonpayload.extra.offer_subcategory_id,
                jsonpayload.extra.offer_booking_number,
                jsonpayload.extra.offer_item_score,
                jsonpayload.extra.offer_item_rank,
                jsonpayload.extra.offer_extra_data.offer_ranking_origin
                as offer_ranking_origin,
                cast(
                    jsonpayload.extra.offer_extra_data.offer_ranking_score as float64
                ) as offer_ranking_score,
                cast(
                    jsonpayload.extra.offer_extra_data.offer_booking_number_last_7_days
                    as float64
                ) as offer_booking_number_last_7_days,
                cast(
                    jsonpayload.extra.offer_extra_data.offer_booking_number_last_14_days
                    as float64
                ) as offer_booking_number_last_14_days,
                cast(
                    jsonpayload.extra.offer_extra_data.offer_booking_number_last_28_days
                    as float64
                ) as offer_booking_number_last_28_days,
                cast(
                    jsonpayload.extra.offer_extra_data.offer_semantic_emb_mean
                    as float64
                ) as offer_semantic_emb_mean
            ) as offer_context,
            jsonpayload.extra.context_extra_data.offer_origin_ids as offer_origin_id,
            jsonpayload.extra.context_extra_data.model_params.name as model_params_name,
            jsonpayload.extra.context_extra_data.model_params.description
            as model_params_description,
            jsonpayload.extra.context_extra_data.scorer.retrievals[
                safe_offset(0)
            ].model_display_name as scorer_retrieval_model_display_name,
            jsonpayload.extra.context_extra_data.scorer.retrievals[
                safe_offset(0)
            ].model_version as scorer_retrieval_model_version,
            jsonpayload.extra.context_extra_data.scorer.ranking.model_display_name
            as scorer_ranking_model_display_name,
            jsonpayload.extra.context_extra_data.scorer.ranking.model_version
            as scorer_ranking_model_version
        from {{ source("raw", "run_googleapis_com_stderr") }} recommendation_sink
        inner join
            {{ ref("int_applicative__offer_item_id") }} int_applicative__offer_item_id
            on recommendation_sink.jsonpayload.extra.offer_id
            = int_applicative__offer_item_id.offer_id
        left join
            {{ ref("int_seed__iris_france") }} ii
            on ii.id = recommendation_sink.jsonpayload.extra.user_iris_id

        where
            resource.type = 'cloud_run_revision'
            and jsonpayload.extra.labels.event_type
            = 'recommendation_past_offer_context_sink'

            and (
                {% if is_incremental() %}
                    date(
                        recommendation_sink.jsonpayload.extra.date
                    ) between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                        '{{ ds() }}'
                    )
                {% else %}
                    date(recommendation_sink.jsonpayload.extra.date)
                    >= date_sub(date('{{ ds() }}'), interval 60 day)
                {% endif %}
            )

        qualify
            row_number() over (
                partition by
                    recommendation_sink.jsonpayload.extra.user_id,
                    recommendation_sink.jsonpayload.extra.call_id,
                    recommendation_sink.jsonpayload.extra.offer_id
                order by recommendation_sink.jsonpayload.extra.date desc
            )
            = 1
    )

select et.*
from export_table et
{% if is_incremental() %}
    where
        et.event_date
        between date_sub(date('{{ ds() }}'), interval 2 day) and date('{{ ds() }}')
{% else %} where et.event_date >= date_sub(date('{{ ds() }}'), interval 60 day)
{% endif %}
