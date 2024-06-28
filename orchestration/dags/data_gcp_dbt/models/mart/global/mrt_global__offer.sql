SELECT
    offer_id,
    offer_product_id,
    offer_product_humanized_id,
    offer_id_at_providers,
    is_synchronised,
    offer_name,
    offer_description,
    offer_category_id,
    last_stock_price,
    offer_creation_date,
    offer_created_at,
    offer_date_updated,
    offer_is_duo,
    item_id,
    offer_is_underage_selectable,
    offer_type_domain,
    offer_is_bookable,
    venue_is_virtual,
    digital_goods,
    physical_goods,
    event,
    offer_humanized_id,
    passculture_pro_url,
    webapp_url,
    offer_subcategory_id,
    offer_url,
    is_national,
    is_active,
    offer_validation,
    author,
    performer,
    stage_director,
    theater_movie_id,
    theater_room_id,
    speaker,
    movie_type,
    visa,
    release_date,
    genres,
    companies,
    countries,
    casting,
    isbn,
    titelive_gtl_id,
    rayon,
    book_editor,
    type,
    sub_type,
    mediation_humanized_id,
    total_individual_bookings,
    total_cancelled_individual_bookings,
    total_used_individual_bookings,
    total_favorites,
    total_stock_quantity,
    total_first_bookings,
    venue_id,
    venue_name,
    venue_department_code,
    venue_label,
    partner_id,
    offerer_id,
    offerer_name,
    venue_type_label,
    venue_iris_internal_id,
    venue_region_name
FROM {{ ref('mrt_global__offer_unverified') }} AS o
WHERE TRUE
    AND offer_validation = 'APPROVED'
