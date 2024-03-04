{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'},
    )
}}

SELECT 
    user.user_id
    , event_date as consultation_date
    , array_agg(distinct offer_item_ids.item_id) as consulted_items
    , array_agg(distinct firebase.origin) as consulted_origins
FROM {{ ref('user_beneficiary') }} user
JOIN {{ source('analytics', 'firebase_events') }} firebase
    ON user.user_id = firebase.user_id
    and event_name = 'ConsultOffer'
    and event_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
JOIN {{ ref('offer_item_ids') }} offer_item_ids -- retire les offres non reliées à un item_id 
    ON firebase.offer_id = offer_item_ids.offer_id
GROUP BY 1, 2