WITH top_items_iris as(
    SELECT
        ti.item_id,
        ti.iris_id,
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_data` ti
),
top_items_inshape as (
    SELECT
        ti.item_id,
        ti.iris_id,
        ro.offer_id,
        ro.venue_id,
    FROM
        top_items_iris ti
        INNER JOIN `{{ bigquery_clean_dataset }}.iris_venues_in_shape` ivr on ti.iris_id = ivr.irisId
        INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro ON ti.item_id = ro.item_id
        and ivr.venueId = ro.venue_id
        and not ro.is_national and ro.url IS NULL
    group by
        1,
        2,
        3,
        4
)
SELECT
    item_id,
    offer_id,
    venue_id,
    iris_id,
    0 as venue_distance_to_iris
FROM
    top_items_inshape