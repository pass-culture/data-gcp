SELECT cs.collective_stock_id,
    cs.stock_id,
    cs.collective_stock_creation_date,
    cs.collective_stock_modification_date,
    cs.collective_stock_beginning_date_time,
    cs.collective_stock_price,
    cs.collective_stock_booking_limit_date_time,
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_price_detail,
    cs.total_non_cancelled_collective_bookings,
    cs.total_collective_bookings,
    cs.total_used_collective_bookings,
    cs.first_collective_booking_date,
    cs.last_collective_booking_date,
    cs.total_collective_theoretic_revenue,
    cs.total_collective_real_revenue,
    cs.collective_stock_is_bookable,
    co.collective_offer_id,
    co.offer_id,
    co.collective_offer_name,
    co.collective_offer_subcategory_id,
    co.collective_offer_category_id,
    co.collective_offer_format,
    co.venue_id,
    co.venue_name,
    co.venue_department_code,
    co.venue_region_name,
    co.offerer_id,
    co.partner_id,
    co.offerer_name,
    co.institution_program_name,
    co.collective_offer_address_type,
    co.collective_offer_image_id,
FROM {{ ref('int_applicative__collective_stock') }} AS cs
INNER JOIN {{ ref('mrt_collective__offer') }} AS co ON cs.collective_offer_id = co.collective_offer_id