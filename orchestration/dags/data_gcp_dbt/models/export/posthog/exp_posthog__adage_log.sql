SELECT  
    event_date, 
    event_name,
    timestamp(event_timestamp) as event_timestamp,
    user_id,
    user_id as user_pseudo_id,
    STRUCT (
        total_results,
        origin,
        collective_stock_id,
        collective_offer_id,
        header_link_name,
        collective_booking_id,
        query_id,
        address_type_filter,
        text_filter,
        department_filter,
        academy_filter,
        artistic_domain_filter,
        student_filter,
        suggestion_type,
        suggestion_value,
        is_favorite,
        venue_filter,
        format_filter,
        playlist_id,
        domain_id,
        rank_clicked,
        venue_id,
        partner_id,
        offer_students,
        offer_format,
        partner_name,
        partner_type,
        partner_status,
        cultural_sector,
        partner_confirmed_collective_bookings,
        partner_department_code
    ) as extra_params,
    STRUCT (
        uai,
        user_role
    )  as user_params,
    'adage' as origin
FROM {{ ref('mrt_collective__adage_log') }}
