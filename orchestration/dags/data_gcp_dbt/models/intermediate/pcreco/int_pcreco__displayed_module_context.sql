{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

SELECT 
    event_date, 
    reco_call_id,
    playlist_origin, 
    offer_origin_id,
    model_params_name,
    model_params_description,
    scorer_retrieval_model_display_name,
    scorer_retrieval_model_version,
    scorer_ranking_model_display_name,
    scorer_ranking_model_version,
    user_context.user_is_geolocated,
    count(distinct offer_id) as total_displayed_offers
FROM {{ ref("int_pcreco__displayed_offer_event")}}

{% if is_incremental() %}   
    WHERE event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}

GROUP BY 
    event_date, 
    reco_call_id,
    playlist_origin, 
    offer_origin_id,
    model_params_name,
    model_params_description,
    scorer_retrieval_model_display_name,
    scorer_retrieval_model_version,
    scorer_ranking_model_display_name,
    scorer_ranking_model_version,
    user_context.user_is_geolocated