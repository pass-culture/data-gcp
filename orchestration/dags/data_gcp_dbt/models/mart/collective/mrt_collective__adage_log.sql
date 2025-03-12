{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

select
    a.event_date,
    a.event_name,
    a.event_timestamp,
    a.user_id,
    a.session_id,
    a.total_results,
    a.origin,
    a.collective_stock_id,
    coalesce(a.collective_offer_id, s.collective_offer_id) as collective_offer_id,
    a.header_link_name,
    a.collective_booking_id,
    a.query_id,
    a.address_type_filter,
    a.text_filter,
    a.department_filter,
    a.academy_filter,
    a.geoloc_radius_filter,
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
    a.source,
    a.venue_id as adage_venue_id,
    o.venue_id as offer_venue_id,
    coalesce(a.venue_id, o.venue_id) as venue_id,
    o.partner_id,
    o.collective_offer_students as offer_students,
    o.collective_offer_format as offer_format,
    p.partner_name,
    p.partner_type,
    p.partner_status,
    p.cultural_sector,
    p.total_non_cancelled_collective_bookings as partner_confirmed_collective_bookings,
    p.partner_department_code,
    a.uai,
    a.user_role
from {{ ref("int_pcapi__adage_log") }} as a
left join
    {{ source("raw", "applicative_database_collective_stock") }} as s
    on s.collective_stock_id = a.collective_stock_id
left join
    {{ ref("mrt_global__collective_offer") }} as o
    on o.collective_offer_id = coalesce(a.collective_offer_id, s.collective_offer_id)
left join {{ ref("mrt_global__cultural_partner") }} as p on p.partner_id = o.partner_id
where
    true
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
