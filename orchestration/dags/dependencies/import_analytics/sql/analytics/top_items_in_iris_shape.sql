CREATE
OR REPLACE TABLE `{{ bigquery_analytics_dataset }}.top_items_in_iris_shape` AS WITH top_items_iris as(
    -- see how we calculate this --> TOP offers only ~ 5K offers per iris
    SELECT
        ti.item_id,
        ti.iris_id,
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_data` ti
),
top_items_inshape as (
    -- get all venues that are within current iris (in iris shape)
    -- TODO : see how iris_venues_raw is calculated
    -- TODO : get pre-calculated distance over iris centroid instead of setting default 0
    -- take all associated offers in this range
    SELECT
        ti.item_id,
        ti.iris_id,
        ro.offer_id,
        ro.venue_id,
    FROM
        top_items_iris ti
        INNER JOIN `{{ bigquery_clean_dataset }}.iris_venues_raw` ivr on ti.iris_id = ivr.irisId
        INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro ON ti.item_id = ro.item_id
        and ivr.venueId = ro.venue_id
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