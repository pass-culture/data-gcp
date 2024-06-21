{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date', "granularity" : "day"},
        on_schema_change = "sync_all_columns",
        cluster_by = "playlist_origin"
    )
}}

WITH displayed AS (
    SELECT
        reco_call_id,
        event_date,
        sum(is_consult_offer) as total_module_consult_offer,
        sum(is_booking_confirmation) as total_module_booking_confirmation,
        sum(is_add_to_favorites) as total_module_add_to_favorites,
    FROM
       {{ ref('int_firebase__native_event') }} fsoe
    WHERE 
        {% if is_incremental() %}   
        event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}') AND
        {% endif %}
        reco_call_id is not null
        AND event_name in (
            "ConsultOffer", 
            "BookingConfirmation", 
            "HasAddedOfferToFavorites", 
            "ModuleDisplayedOnHomePage",
            "PlaylistHorizontalScroll",
            "PlaylistVerticalScroll"
        )
    GROUP BY reco_call_id, event_date
)

SELECT 
    et.*, 
    d.total_module_consult_offer, 
    d.total_module_booking_confirmation, 
    d.total_module_add_to_favorites, 
FROM {{ ref('int_pcreco__past_offer_context')}} et
INNER JOIN displayed d ON d.event_date = et.event_date AND d.reco_call_id = et.reco_call_id
{% if is_incremental() %}   
    WHERE et.event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}