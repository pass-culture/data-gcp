-- create tmp materialized table
drop function if exists get_non_recommendable_items_{{ ts_nodash }}
cascade
;
create or replace function get_non_recommendable_items_{{ ts_nodash }} ()
returns table(user_id varchar, item_id varchar)
as $body$
BEGIN
    RETURN QUERY
    SELECT *
    FROM public.non_recommendable_items_data;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS non_recommendable_items_mv_tmp AS
SELECT * FROM get_non_recommendable_items_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX idx_non_recommendable_user_item_id_tmp_{{ ts_nodash  }}  ON public.non_recommendable_items_mv_tmp USING btree (user_id,item_id);

CREATE INDEX idx_non_recommendable_item_id_tmp_{{ ts_nodash  }}  ON public.non_recommendable_items_mv_tmp(user_id);

-- Refresh state
refresh materialized view non_recommendable_items_mv_tmp
;

-- Move tmp to final Materialized view
-- mv -> mv_old
-- mv_tmp -> mv
-- drop mv_old if exists
ALTER MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv
    RENAME TO non_recommendable_items_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_tmp
    RENAME TO non_recommendable_items_mv;
DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_old;
