WITH selected_items AS (
SELECT
        *,
        'in' as position
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_in_iris_shape`
    UNION
    ALL
    SELECT
        *,
        'out' as position
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_out_iris_shape`
)
select
    si.item_id,
    si.offer_id,
    ro.product_id,
    ro.category,
    ro.subcategory_id,
    ro.search_group_name,
    si.iris_id,
    si.venue_id,
    si.venue_distance_to_iris,
    ro.name,
    ro.url,
    ro.is_national,
    ro.offer_creation_date,
    ro.stock_beginning_date,
    ro.stock_price,
    ro.booking_number,
    ro.is_underage_recommendable,
    position
FROM
    selected_items si
    INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro ON ro.offer_id = si.offer_id -- get distinct tables with distinct heuristics
    GROUP by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9