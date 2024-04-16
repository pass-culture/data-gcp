{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

SELECT  
    a.event_date,
    a.event_name,
    a.event_timestamp,
    a.user_id,
    a.session_id,
    a.total_results,
    a.origin,
    a.collective_stock_id,
    COALESCE(a.collective_offer_id,s.collective_offer_id) as collective_offer_id,
    a.header_link_name,
    a.collective_booking_id,
    a.query_id,
    a.address_type_filter,
    a.text_filter,
    a.department_filter,
    a.academy_filter,
    a.artistic_domain_filter,
    a.student_filter,
    a.suggestion_type,
    a.suggestion_value,
    a.is_favorite,
    a.venue_filter,
    a.format_filter,
    a.playlist_id,
    a.domain_id,
    a.rank_clicked,
    a.venue_id AS adage_venue_id,
    o.venue_id AS offer_venue_id,
    COALESCE(a.venue_id,o.venue_id) AS venue_id,
    o.partner_id,
    o.collective_offer_students as offer_students,
    o.collective_offer_format as offer_format,
    p.partner_name,
    p.partner_type,
    p.partner_status,
    p.cultural_sector,
    p.confirmed_collective_bookings as partner_confirmed_collective_bookings,
    p.partner_department_code,
    a.uai,
    a.user_role,
FROM {{ ref("int_pcapi__adage_log") }} AS a
LEFT JOIN {{ source("raw","applicative_database_collective_stock") }} AS s ON s.collective_stock_id = a.collective_stock_id
LEFT JOIN {{ ref("enriched_collective_offer_data") }} AS o ON o.collective_offer_id = COALESCE(a.collective_offer_id,s.collective_offer_id)
LEFT JOIN {{ ref("enriched_cultural_partner_data") }} AS p ON p.partner_id = o.partner_id
WHERE TRUE
{% if is_incremental() %}
AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
{% endif %}
