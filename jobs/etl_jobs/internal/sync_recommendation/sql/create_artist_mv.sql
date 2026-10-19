create or replace function get_artist_{{ ts_nodash }} ()
returns
    table(
        artist_id varchar,
        main_search_group_name varchar,
        main_search_group_name_has_bookable_offer boolean
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT
        artist.artist_id::varchar as artist_id,
        artist.main_search_group_name::varchar as main_search_group_name,
        artist.main_search_group_name_has_bookable_offer::boolean
            as main_search_group_name_has_bookable_offer
    FROM public.artist artist;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS artist_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS artist_mv_tmp AS
SELECT * FROM get_artist_{{ ts_nodash  }}()
WITH NO DATA;

-- Create indexes
CREATE UNIQUE INDEX idx_artist_mv_{{ ts_nodash }} ON public.artist_mv_tmp USING btree (artist_id);

-- Refresh state
refresh materialized view artist_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS artist_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS artist_mv
    RENAME TO artist_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS artist_mv_tmp
    RENAME TO artist_mv;
DROP MATERIALIZED VIEW IF EXISTS artist_mv_old;
commit
;

-- Cleanup orphaned functions left by previous runs (scheduled or manual).
-- The function still backing the freshly renamed materialized view is
-- automatically preserved: DROP FUNCTION without CASCADE fails while a
-- dependent object exists, so it is simply skipped.
create or replace function cleanup_get_artist_functions_{{ ts_nodash }} ()
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
          AND p.proname ~ '^get_artist_[0-9]{14}$'
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

select cleanup_get_artist_functions_{{ ts_nodash }} ()
;
drop function cleanup_get_artist_functions_{{ ts_nodash }} ()
;
