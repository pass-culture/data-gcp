WITH top_items AS(
    SELECT
        inn.item_id,
        inn.subcategory_id,
        ROW_NUMBER() OVER (
            PARTITION BY inn.subcategory_id
            ORDER BY
                booking_number DESC
        ) AS rank
    FROM
        (
            SELECT
                ro.item_id,
                ro.subcategory_id,
                MAX(CAST(ro.url IS NOT NULL AS int)) AS is_numerical,
                MAX(ro.booking_number) AS booking_number
            FROM
                `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro
            where
                ro.booking_number > 0
                and (
                    is_national
                    or ro.url IS NOT NULL
                )
            GROUP BY
                ro.item_id,
                ro.subcategory_id
        ) inn
)
SELECT
    ti.item_id,
    ro.offer_id,
    'None' as venue_id,
    'None' as iris_id,
    0 as venue_distance_to_iris
FROM top_items ti
INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro ON ti.item_id = ro.item_id
WHERE rank < 1000