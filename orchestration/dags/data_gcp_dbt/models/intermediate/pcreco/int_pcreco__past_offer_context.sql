{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns",
        cluster_by = "playlist_origin",
    )
) }}

WITH export_table AS (
    SELECT
        pso.id,
        date(date) as event_date,
        date as event_created_at,
        call_id as reco_call_id,
        CASE
            WHEN context like "similar_offer:%" THEN "similar_offer"
            WHEN context like "recommendation_fallback:%" THEN "similar_offer"
            WHEN context like "recommendation:%" THEN "recommendation"
        ELSE "unknown"
        END as playlist_origin,
        context,
        round(offer_order) as offer_display_order,
        CAST(user_id AS STRING) as user_id,
        CAST(offer_id as STRING) as offer_id,
        offer_item_ids.item_id as item_id,
        STRUCT(
          user_deposit_remaining_credit,
          user_bookings_count,
          user_clicks_count,
          user_favorites_count,
          user_iris_id,
          ii.centroid as user_iris_centroid,
          user_is_geolocated
        ) as user_context,
        STRUCT(
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
          REPLACE(JSON_EXTRACT(offer_extra_data, "$.offer_ranking_origin"),  '"', '') as offer_ranking_origin,
          CAST(JSON_EXTRACT(offer_extra_data, "$.offer_ranking_score") AS FLOAT64) as offer_ranking_score,
          CAST(JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_7_days")  AS FLOAT64) as offer_booking_number_last_7_days,
          CAST(JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_14_days")  AS FLOAT64) as offer_booking_number_last_14_days,
          CAST(JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_28_days") AS FLOAT64) as offer_booking_number_last_28_days,
          CAST(JSON_EXTRACT(offer_extra_data, "$.offer_semantic_emb_mean") AS FLOAT64) as offer_semantic_emb_mean
        ) as offer_context,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.offer_origin_id"),  '"', '') as offer_origin_id,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.model_params.name"),  '"', '') as model_params_name,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.model_params.description"),  '"', '') as model_params_description,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.scorer.retrievals[0].model_display_name"),  '"', '') as scorer_retrieval_model_display_name,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.scorer.retrievals[0].model_version"),  '"', '') as scorer_retrieval_model_version,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.scorer.ranking.model_display_name"),  '"', '') as scorer_ranking_model_display_name,
        REPLACE(JSON_EXTRACT(context_extra_data, "$.scorer.ranking.model_version"),  '"', '') as scorer_ranking_model_version
    FROM
       {{ source('raw', 'past_offer_context') }} pso
    INNER JOIN {{ ref('offer_item_ids') }} offer_item_ids USING(offer_id)
    LEFT JOIN {{ source('clean', 'iris_france') }}  ii on ii.id = pso.user_iris_id

    {% if is_incremental() %}
        WHERE import_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
    {% else %}
        WHERE import_date >= date_sub(DATE('{{ ds() }}'), INTERVAL 60 DAY)
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            user_id,
            call_id,
            offer_id
        ORDER BY
            date DESC
        ) = 1
)

SELECT
    et.*
FROM export_table et
{% if is_incremental() %}
    WHERE et.event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 2 DAY) and DATE('{{ ds() }}')
{% else %}
    WHERE et.event_date >= date_sub(DATE('{{ ds() }}'), INTERVAL 60 DAY)
{% endif %}
