/* Create the function to fetch the non recommendable offers.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_non_recommendable_items CASCADE;
CREATE OR REPLACE FUNCTION get_non_recommendable_items()
RETURNS TABLE (user_id varchar,
               item_id varchar) AS
$body$
BEGIN
    RETURN QUERY
    SELECT * from public.non_recommendable_items_data;
END;
$body$
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items;
CREATE MATERIALIZED VIEW IF NOT EXISTS non_recommendable_items
AS
    SELECT * from get_non_recommendable_items()
WITH NO DATA;

CREATE UNIQUE INDEX idx_non_recommendable_item_id ON public.non_recommendable_items USING btree (user_id,item_id);
REFRESH MATERIALIZED VIEW non_recommendable_items;