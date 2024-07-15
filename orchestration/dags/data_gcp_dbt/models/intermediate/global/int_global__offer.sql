{{
    config(
        partition_by={
            "field": "offer_creation_date",
            "data_type": "date"
        },
        on_schema_change = "sync_all_columns"
    )
}}

SELECT
    o.offer_id,
    o.offer_product_id,
    o.offer_product_humanized_id,
    o.offer_id_at_providers,
    o.is_synchronised,
    o.offer_name,
    o.offer_description,
    o.offer_category_id,
    o.last_stock_price,
    o.offer_creation_date,
    o.offer_created_at,
    o.offer_date_updated,
    o.offer_is_duo,
    o.item_id,
    o.offer_is_underage_selectable,
    o.offer_type_domain,
    o.offer_is_bookable,
    v.venue_is_virtual,
    o.digital_goods,
    o.physical_goods,
    o.event,
    o.offer_humanized_id,
    o.passculture_pro_url,
    o.webapp_url,
    o.offer_subcategory_id,
    o.offer_url,
    o.is_national,
    o.is_active,
    o.offer_validation,
    o.author,
    o.performer,
    o.stage_director,
    o.theater_movie_id,
    o.theater_room_id,
    o.speaker,
    o.movie_type,
    o.visa,
    o.release_date,
    o.genres,
    o.companies,
    o.countries,
    o.casting,
    o.isbn,
    o.titelive_gtl_id,
    o.rayon,
    o.book_editor,
    o.type,
    o.subType AS sub_type,
    o.mediation_humanized_id,
    o.total_individual_bookings,
    o.total_cancelled_individual_bookings,
    o.total_used_individual_bookings,
    o.total_favorites,
    o.total_stock_quantity,
    o.total_first_bookings,
    v.venue_id,
    v.venue_name,
    v.venue_department_code,
    v.venue_label,
    v.partner_id,
    v.offerer_id,
    v.offerer_name,
    v.venue_type_label,
    v.venue_iris_internal_id,
    v.venue_region_name,
    o.offerer_address_id,
    o.offer_publication_date,
    o.is_future_scheduled
FROM {{ ref('int_applicative__offer') }} AS o
INNER JOIN {{ ref('int_global__venue')}} AS v ON v.venue_id = o.venue_id