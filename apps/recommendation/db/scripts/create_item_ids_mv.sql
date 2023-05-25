/* Create the function to fetch the non recommendable offers.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_item_ids CASCADE;

CREATE
OR REPLACE FUNCTION get_item_ids() RETURNS TABLE (
    offer_id varchar,
    item_id varchar,
    booking_number int
) AS $body$ BEGIN RETURN QUERY
SELECT
    distinct ro.offer_id
    ,ro.item_id
    ,ro.booking_number
FROM
    public.recommendable_offers_raw ro;

END;

$body$ LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS item_ids_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS item_ids_mv AS
SELECT
    *
from
    get_item_ids() WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_item_ids_mv ON public.item_ids_mv USING btree (offer_id);

REFRESH MATERIALIZED VIEW item_ids_mv;