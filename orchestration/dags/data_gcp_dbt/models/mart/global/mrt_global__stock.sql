{{
    config(
        partition_by={
            "field": "stock_modified_date",
            "data_type": "date"
        },
        on_schema_change = "sync_all_columns"
    )
}}

SELECT
    s.stock_beginning_date,
    s.stock_modified_date,
    s.stock_modified_at,
    s.stock_id,
    s.price_category_id,
    s.stock_creation_date,
    s.stock_booking_limit_date,
    s.total_available_stock,
    s.stock_quantity,
    s.total_bookings,
    s.total_non_cancelled_bookings,
    s.total_cancelled_bookings,
    s.total_paid_bookings,
    s.stock_price,
    s.price_category_label_id,
    s.price_category_label,
    s.stock_features,
    s.offerer_address_id,
    o.offer_id,
    o.offer_product_id,
    o.offer_id_at_providers,
    o.offer_name,
    o.offer_description,
    o.offer_subcategory_id,
    o.offer_creation_date,
    o.offer_is_duo,
    o.item_id,
    o.venue_id,
    o.venue_name,
    o.venue_department_code,
    o.venue_label,
    o.venue_type_label,
    o.offerer_id,
    o.offerer_name,
    o.partner_id,
    o.physical_goods,
    o.digital_goods,
    o.event,
    o.offer_category_id,
    o.venue_iris_internal_id
FROM {{ref('int_applicative__stock')}} AS s
LEFT JOIN {{ref('int_global__offer')}} AS o ON s.offer_id = o.offer_id