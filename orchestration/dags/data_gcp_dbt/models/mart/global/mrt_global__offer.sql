select
    offer_id,
    offer_product_id,
    offer_product_humanized_id,
    offer_id_at_providers,
    offer_last_provider_id,
    is_synchronised,
    offer_name,
    offer_description,
    offer_category_id,
    last_stock_price,
    offer_creation_date,
    offer_created_at,
    offer_updated_date,
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
    venue_department_name,
    venue_region_name,
    venue_postal_code,
    venue_city,
    venue_epci,
    venue_academy_name,
    venue_density_label,
    venue_macro_density_label,
    venue_density_level,
    venue_label,
    venue_is_permanent,
    partner_id,
    offerer_id,
    venue_managing_offerer_id,
    offerer_name,
    venue_type_label,
    venue_iris_internal_id,
    offerer_address_id,
    offer_publication_date,
    is_future_scheduled
from {{ ref("int_global__offer") }} as o
where true and offer_validation = 'APPROVED' and venue_id is not null
