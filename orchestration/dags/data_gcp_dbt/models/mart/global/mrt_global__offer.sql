SELECT
    o.offer_id,
    o.offer_product_id,
    o.offer_id_at_providers,
    o.offer_name,
    o.offer_description,
    o.offer_subcategoryId,
    o.offer_creation_date,
    o.offer_is_duo,
    v.venue_id,
    v.venue_name,
    v.venue_department_code,
FROM {{ref('int_applicative__offer')}} AS o
LEFT JOIN {{ref('mrt_global__venue')}} AS v ON v.venue_id = o.venue_id