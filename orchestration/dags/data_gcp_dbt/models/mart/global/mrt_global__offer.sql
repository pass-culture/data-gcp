{{
    config(
        materialized = "incremental",
        unique_key = "offer_id",
        on_schema_change = "sync_all_columns"
    )
}}

SELECT
    o.offer_id,
    o.offer_product_id,
    o.offer_id_at_providers,
    o.offer_name,
    o.offer_description,
    o.offer_creation_date,
    o.offer_date_updated,
    o.offer_is_duo,
    o.offer_subcategoryId, -- diff entre category et subcategory?
    o.physical_goods,
    o.digital_goods,
    o.event,
    o.offer_category_id,
    o.item_id,
    v.venue_id,
    v.venue_name,
    v.venue_department_code,
    v.venue_label,
    v.partner_id,
    v.venue_managing_offerer_id AS offerer_id,
    v.offerer_name,
    v.venue_type_label,
FROM {{ref('int_applicative__offer')}} AS o
INNER JOIN {{ref('int_applicative__venue')}} AS v ON v.venue_id = o.venue_id
WHERE TRUE
    {% if is_incremental() %}
    AND offer_date_updated BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
    {% endif %}
