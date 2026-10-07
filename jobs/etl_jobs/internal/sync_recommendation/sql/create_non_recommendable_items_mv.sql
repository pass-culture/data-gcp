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

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv
    RENAME TO non_recommendable_items_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_tmp
    RENAME TO non_recommendable_items_mv;
DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items_mv_old;
commit
;

-- Cleanup orphaned functions left by previous runs (scheduled or manual).
-- The function still backing the freshly renamed materialized view is
-- automatically preserved: DROP FUNCTION without CASCADE fails while a
-- dependent object exists, so it is simply skipped.
create or replace function
    cleanup_get_non_recommendable_items_functions_{{ ts_nodash }} ()
returns void
as $body$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure AS func_sig
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public'
          AND p.proname ~ '^get_non_recommendable_items_[0-9]{14}$'
    LOOP
        BEGIN
            EXECUTE format('DROP FUNCTION %s', r.func_sig);
        EXCEPTION WHEN dependent_objects_still_exist THEN
            -- still referenced by the current materialized view, skip it
            NULL;
        END;
    END LOOP;
END;
$body$
language plpgsql
;

select cleanup_get_non_recommendable_items_functions_{{ ts_nodash }} ()
;
drop function cleanup_get_non_recommendable_items_functions_{{ ts_nodash }} ()
;
