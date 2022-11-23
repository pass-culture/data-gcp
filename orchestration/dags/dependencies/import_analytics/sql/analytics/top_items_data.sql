WITH top_items AS(
    SELECT
        inn.item_id,
        inn.subcategory_id,
        inn.iris_id,
        ROW_NUMBER() OVER (
            PARTITION BY inn.iris_id,
            inn.subcategory_id
            ORDER BY
                is_numerical ASC,
                booking_number DESC
        ) AS rank
    FROM
        (
            SELECT
                ro.item_id,
                ro.subcategory_id,
                iv.irisId AS iris_id,
                MAX(CAST(ro.url IS NOT NULL AS int)) AS is_numerical,
                MAX(ro.booking_number) AS booking_number
            FROM
                `{{ bigquery_analytics_dataset }}`.recommendable_offers_data ro
                LEFT JOIN `{{ bigquery_analytics_dataset }}`.iris_venues_at_radius iv ON ro.venue_id = iv.venueId
            where
                ro.booking_number > 0
            GROUP BY
                ro.item_id,
                ro.subcategory_id,
                iv.irisId
        ) inn
)
SELECT
    *
FROM
    top_items
WHERE
    rank < 2000