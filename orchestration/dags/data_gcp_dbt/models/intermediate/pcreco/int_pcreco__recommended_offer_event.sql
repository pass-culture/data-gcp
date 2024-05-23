{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns",
        cluster_by = "playlist_origin"
    )
}}

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
        offer_order as offer_display_order,        
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
          JSON_EXTRACT(offer_extra_data, "$.offer_ranking_score") as offer_ranking_score,
          REPLACE(JSON_EXTRACT(offer_extra_data, "$.offer_ranking_origin"),  '"', '') as offer_ranking_origin,
          JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_7_days") as offer_booking_number_last_7_days,
          JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_14_days") as offer_booking_number_last_14_days,
          JSON_EXTRACT(offer_extra_data, "$.offer_booking_number_last_28_days") as offer_booking_number_last_28_days,
          JSON_EXTRACT(offer_extra_data, "$.offer_semantic_emb_mean") as offer_semantic_emb_mean
        ) as offer_context,
        STRUCT(
            JSON_EXTRACT(context_extra_data, "$.offer_origin_id") as offer_origin_id
        ) as recommendation_context        
    FROM
       {{ source('raw', 'past_offer_context') }} pso 
    INNER JOIN {{ ref('offer_item_ids') }} offer_item_ids USING(offer_id)
    INNER JOIN {{ source('clean', 'iris_france') }}  ii on ii.id = pso.user_iris_id

    {% if is_incremental() %}   
        WHERE import_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3+2 DAY) and DATE('{{ ds() }}')
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

SELECT * 
FROM export_table
{% if is_incremental() %}   
    WHERE event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}