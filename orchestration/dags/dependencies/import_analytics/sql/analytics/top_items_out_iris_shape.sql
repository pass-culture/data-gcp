with top_items_iris as(
    SELECT
        ti.item_id,
        ti.iris_id,
    FROM
        `{{ bigquery_analytics_dataset }}.top_items_data` ti
),
top_items_out_shape as(
    SELECT
        ti.item_id,
        ti.iris_id
    FROM
        top_items_iris ti 
        LEFT JOIN `{{ bigquery_analytics_dataset }}.top_items_in_iris_shape` ti_in on ti.item_id = ti_in.item_id
        and ti.iris_id = ti_in.iris_id
    WHERE
        ti_in.item_id is null
),
top_items_outshape_w_distance as(
    SELECT
        item_out.item_id,
        ro.offer_id,
        v.venue_id,
        item_out.iris_id,
        case
            when (v.venue_latitude is not null) then ST_DISTANCE(
                irisf.centroid,
                ST_GEOGPOINT(v.venue_longitude, v.venue_latitude)
            )
            ELSE null
        END as venue_distance_to_iris
    FROM
        top_items_out_shape item_out 
        INNER JOIN `{{ bigquery_analytics_dataset }}.iris_france` irisf on item_out.iris_id = irisf.id 
        INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_offers_data` ro ON item_out.item_id = ro.item_id 
        INNER JOIN `{{ bigquery_clean_dataset }}.applicative_database_venue` v on ro.venue_id = v.venue_id
    WHERE not ro.is_national and ro.url IS NULL
),
item_out_rank_by_distance as(
    SELECT
        item_id,
        offer_id,
        venue_id,
        iris_id,
        venue_distance_to_iris
    FROM
        top_items_outshape_w_distance QUALIFY ROW_NUMBER() OVER (
            PARTITION BY iris_id,
            item_id
            ORDER BY
                venue_distance_to_iris ASC
        ) = 1
)
SELECT
    item_id,
    offer_id,
    venue_id,
    iris_id,
    venue_distance_to_iris
FROM
    item_out_rank_by_distance