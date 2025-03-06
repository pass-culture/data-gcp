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
        )
    )
}}

select
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
from {{ ref("int_pcreco__displayed_offer_event") }}

{% if is_incremental() %}
    where
        event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
{% endif %}

group by
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
