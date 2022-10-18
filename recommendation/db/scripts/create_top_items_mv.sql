/* Create the function to fetch the top_items from the source table.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_top_items CASCADE;

CREATE OR REPLACE FUNCTION get_top_items() 
RETURNS TABLE ( item_id varchar,
                subcategory_id varchar,
                iris_id varchar,
                rank integer) AS 
$body$ 
BEGIN 
    RETURN QUERY
    SELECT * from public.top_items;
END;
$body$ 
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS top_items_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS top_items_mv AS
SELECT * FROM get_top_items() 
WITH NO DATA;

CREATE UNIQUE INDEX idx_top_items_mv_irisid ON public.top_items_mv USING btree (item_id, iris_id);
REFRESH MATERIALIZED VIEW top_items_mv;

/*
 NB: REFRESH CONCURRENTLY is slower than a normal refresh : 1 minute vs 2 minutes
 */