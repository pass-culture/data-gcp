create or replace function get_artist_top_by_category_{{ ts_nodash }} ()
returns
    table(
        artist_id varchar,
        search_group_name varchar,
        score_name varchar,
        score_value real,
        category_rank integer
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT
        artist_top_by_category.artist_id::varchar as artist_id,
        artist_top_by_category.search_group_name::varchar as search_group_name,
        artist_top_by_category.score_name::varchar as score_name,
        artist_top_by_category.score_value::real as score_value,
        artist_top_by_category.category_rank::integer as category_rank
    FROM public.artist_top_by_category artist_top_by_category;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS artist_top_by_category_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS artist_top_by_category_mv_tmp AS
SELECT * FROM get_artist_top_by_category_{{ ts_nodash  }}()
WITH NO DATA;

-- Create indexes
-- One row per (search_group_name, score_name, category_rank): lets the API
-- fetch "top N artists of a category for a given score_name" directly, and
-- also supports switching score_name at read time (A/B test) without
-- re-importing the table.
CREATE UNIQUE INDEX idx_artist_top_by_category_mv_{{ ts_nodash }}
    ON public.artist_top_by_category_mv_tmp
    USING btree (search_group_name, score_name, category_rank);

-- Refresh state
refresh materialized view artist_top_by_category_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS artist_top_by_category_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS artist_top_by_category_mv
    RENAME TO artist_top_by_category_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS artist_top_by_category_mv_tmp
    RENAME TO artist_top_by_category_mv;
DROP MATERIALIZED VIEW IF EXISTS artist_top_by_category_mv_old;
commit
;

-- Cleanup orphaned functions left by previous runs (scheduled or manual).
-- The function still backing the freshly renamed materialized view is
-- automatically preserved: DROP FUNCTION without CASCADE fails while a
-- dependent object exists, so it is simply skipped.
create or replace function
    cleanup_get_artist_top_by_category_functions_{{ ts_nodash }} ()
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
          AND p.proname ~ '^get_artist_top_by_category_[0-9]{14}$'
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

select cleanup_get_artist_top_by_category_functions_{{ ts_nodash }} ()
;
drop function cleanup_get_artist_top_by_category_functions_{{ ts_nodash }} ()
;
