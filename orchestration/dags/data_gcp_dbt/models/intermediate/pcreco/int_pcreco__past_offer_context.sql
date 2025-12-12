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
    export_table as (
        select
            pso.id,
            date(date) as event_date,
            date as event_created_at,
            call_id as reco_call_id,
            case
                when context like "similar_offer:%"
                then "similar_offer"
                when context like "recommendation_fallback:%"
                then "similar_offer"
                when context like "recommendation:%"
                then "recommendation"
                else "unknown"
            end as playlist_origin,
            context,
            round(offer_order) as offer_display_order,
            cast(user_id as string) as user_id,
            cast(offer_id as string) as offer_id,
            int_applicative__offer_item_id.item_id as item_id,
            struct(
                user_deposit_remaining_credit,
                user_bookings_count,
                user_clicks_count,
                user_favorites_count,
                user_iris_id,
                ii.centroid as user_iris_centroid,
                user_is_geolocated
            ) as user_context,
            struct(
                offer_user_distance,
                offer_is_geolocated,
                offer_stock_price,
                offer_creation_date,
                offer_stock_beginning_date,
                offer_category,
                offer_subcategory_id,
                offer_booking_number,
                offer_item_score,
                offer_item_rank,
                replace(
                    json_extract(offer_extra_data, "$.offer_ranking_origin"), '"', ''
                ) as offer_ranking_origin,
                cast(
                    json_extract(offer_extra_data, "$.offer_ranking_score") as float64
                ) as offer_ranking_score,
                cast(
                    json_extract(
                        offer_extra_data, "$.offer_booking_number_last_7_days"
                    ) as float64
                ) as offer_booking_number_last_7_days,
                cast(
                    json_extract(
                        offer_extra_data, "$.offer_booking_number_last_14_days"
                    ) as float64
                ) as offer_booking_number_last_14_days,
                cast(
                    json_extract(
                        offer_extra_data, "$.offer_booking_number_last_28_days"
                    ) as float64
                ) as offer_booking_number_last_28_days,
                cast(
                    json_extract(
                        offer_extra_data, "$.offer_semantic_emb_mean"
                    ) as float64
                ) as offer_semantic_emb_mean
            ) as offer_context,
            replace(
                json_extract(context_extra_data, "$.offer_origin_ids"), '"', ''
            ) as offer_origin_id,
            replace(
                json_extract(context_extra_data, "$.model_params.name"), '"', ''
            ) as model_params_name,
            replace(
                json_extract(context_extra_data, "$.model_params.description"), '"', ''
            ) as model_params_description,
            replace(
                json_extract(
                    context_extra_data, "$.scorer.retrievals[0].model_display_name"
                ),
                '"',
                ''
            ) as scorer_retrieval_model_display_name,
            replace(
                json_extract(
                    context_extra_data, "$.scorer.retrievals[0].model_version"
                ),
                '"',
                ''
            ) as scorer_retrieval_model_version,
            replace(
                json_extract(context_extra_data, "$.scorer.ranking.model_display_name"),
                '"',
                ''
            ) as scorer_ranking_model_display_name,
            replace(
                json_extract(context_extra_data, "$.scorer.ranking.model_version"),
                '"',
                ''
            ) as scorer_ranking_model_version
        from {{ source("raw", "past_offer_context") }} pso
        inner join
            {{ ref("int_applicative__offer_item_id") }} int_applicative__offer_item_id
            using (offer_id)
        left join {{ ref("int_seed__iris_france") }} ii on ii.id = pso.user_iris_id

        {% if is_incremental() %}
            where
                import_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
        {% else %} where import_date >= date_sub(date('{{ ds() }}'), interval 60 day)
        {% endif %}

        qualify
            row_number() over (
                partition by user_id, call_id, offer_id order by date desc
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
