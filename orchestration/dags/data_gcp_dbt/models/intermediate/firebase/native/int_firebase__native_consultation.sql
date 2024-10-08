{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns",
        require_partition_filter = true
    )
) }}

SELECT DISTINCT
    CONCAT(user_id, "-", event_timestamp, "-", offer_id) AS consultation_id,
    user_id,
    event_date AS consultation_date,
    event_timestamp AS consultation_timestamp,
    offer_id,
    origin,
    module_id,
    unique_session_id,
    event_name,
    venue_id,
    traffic_medium,
    traffic_campaign,
    CASE
            WHEN EXISTS (
                SELECT 1
                FROM {{ source('raw', 'subcategories') }} sc
                WHERE LOWER(query) LIKE CONCAT('%', LOWER(sc.category_id), '%')
                    OR LOWER(query) LIKE CONCAT('%', LOWER(sc.id), '%')
            )
            OR EXISTS (
                SELECT 1
                FROM {{ source('seed', 'macro_rayons') }} mr
                WHERE LOWER(query) LIKE CONCAT('%', LOWER(mr.macro_rayon), '%')
                    OR LOWER(query) LIKE CONCAT('%', LOWER(mr.rayon), '%')
            )
            THEN TRUE
            ELSE FALSE
        END AS search_query_input_is_generic,
    query,
    module_id,
    entry_id,
    home_type,
    similar_offer_id,
    similar_offer_playlist_type,
    multi_venue_offer_id
FROM {{ ref('int_firebase__native_event') }}
WHERE event_name = 'ConsultOffer'
    AND user_id IS NOT NULL
    AND offer_id IS NOT NULL
    {% if is_incremental() %}
    AND date(event_date) >= date_sub('{{ ds() }}', INTERVAL 3 day)
    {% else %}
    AND date(event_date) >= date_sub('{{ ds() }}', INTERVAL 1 year)
    {% endif %}
