{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
    )
}}

SELECT 
    event_date, 
    reco_call_id,
    playlist_origin, 
    context,
    recommendation_context.offer_origin_id,
    user_context.user_is_geolocated,
    count(distinct offer_id) as total_displayed_offers
FROM {{ ref("int_pcreco__recommended_offer_event")}}

{% if is_incremental() %}   
    WHERE event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}


GROUP BY 1,2,3,4,5,6