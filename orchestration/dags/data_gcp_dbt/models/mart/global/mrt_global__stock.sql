SELECT
    s.stock_beginning_date,
    s.stock_id,
    o.offer_id,
    o.offer_product_id,
    o.offer_id_at_providers,
    o.offer_name,
    o.offer_description,
    o.offer_subcategoryId,
    o.offer_creation_date,
    o.offer_is_duo,
    o.venue_id,
    o.venue_name,
    o.venue_department_code,
FROM {{ref('int_applicative__stock')}} AS s
LEFT JOIN {{ref('mrt_global__offer')}} AS o ON v.stock_id = o.stock_id