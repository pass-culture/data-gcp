{{
    config(
        materialized = "incremental",
        unique_key = "stock_id",
        on_schema_change = "sync_all_columns"
    )
}}

SELECT
    s.stock_beginning_date,
    s.stock_modified_date,
    s.stock_id,
    o.offer_id,
    o.offer_product_id,
    o.offer_id_at_providers,
    o.offer_name,
    o.offer_description,
    o.offer_subcategoryId,
    o.offer_creation_date,
    o.offer_is_duo,
    o.item_id,
    o.offer_url,
    o.last_stock_price,
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
    o.venue_latitude,
    o.venue_longitude,
    o.offer_is_national
FROM {{ref('int_applicative__stock')}} AS s
INNER JOIN {{ref('mrt_global__offer')}} AS o ON s.offer_id = o.offer_id
WHERE TRUE
    {% if is_incremental() %}
    AND stock_modified_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
    {% endif %}
