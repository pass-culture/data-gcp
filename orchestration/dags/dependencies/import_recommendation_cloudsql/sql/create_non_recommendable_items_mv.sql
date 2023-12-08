
-- create tmp materialized table
DROP FUNCTION IF EXISTS get_non_recommendable_items_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_non_recommendable_items_{{ ts_nodash  }}()
RETURNS TABLE (   
                user_id varchar,
                item_id varchar
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT *
    FROM public.non_recommendable_items_data;
END;
$body$
LANGUAGE plpgsql;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS non_recommendable_items_mv_tmp AS
SELECT * FROM get_non_recommendable_items_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX idx_non_recommendable_user_item_id_tmp_{{ ts_nodash  }}  ON public.non_recommendable_items_mv_tmp USING btree (user_id,item_id);

CREATE INDEX idx_non_recommendable_item_id_tmp_{{ ts_nodash  }}  ON public.non_recommendable_items_mv_tmp(user_id);

-- Refresh state
REFRESH MATERIALIZED VIEW non_recommendable_items_mv_tmp;
