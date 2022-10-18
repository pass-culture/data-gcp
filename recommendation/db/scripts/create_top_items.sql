/* Create the function to fetch the top_items from the source table.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_top_items CASCADE;

CREATE OR REPLACE FUNCTION get_top_items() RETURNS TABLE (
    item_id varchar,
    subcategory_id varchar,
    iris_id varchar,
    row_number bigint 
) AS $body$ BEGIN RETURN QUERY
SELECT 
inn.item_id,
inn.subcategory_id,
inn.iris_id,
row_number() over (
    partition by inn.iris_id,
    inn.subcategory_id
    order by
        is_numerical ASC,
        booking_number DESC
) as rnk
from
    (
        SELECT
            ro.item_id,
            ro.subcategory_id,
            iv.iris_id,
            max(cast(ro.url is not null as int)) as is_numerical,
            max(ro.booking_number) as booking_number
        from
            (
                select
                    *
                from
                    public.recommendable_offers
            ) ro
            left JOIN public.iris_venues_mv iv on ro.venue_id = iv.venue_id
        group by
            ro.item_id,
            ro.subcategory_id,
            iv.iris_id
    ) inn;
END;

$body$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW IF NOT EXISTS top_items_mv AS
SELECT
    *
from
    get_top_items() WITH NO DATA;

CREATE UNIQUE INDEX idx_top_items_mv_irisid ON public.top_items_mv USING btree (item_id, iris_id);

REFRESH MATERIALIZED VIEW top_items_mv;

/*
 NB: REFRESH CONCURRENTLY is slower than a normal refresh : 1 minute vs 2 minutes
 */