create or replace function get_enriched_user_{{ ts_nodash }} ()
returns
    table(
        user_id varchar,
        user_deposit_creation_date timestamp,
        user_birth_date timestamp,
        user_deposit_initial_amount real,
        user_theoretical_remaining_credit real,
        booking_cnt integer,
        consult_offer integer,
        has_added_offer_to_favorites integer
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT *
    FROM public.enriched_user;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_mv_tmp AS
SELECT * FROM get_enriched_user_{{ ts_nodash  }}()
WITH NO DATA;


CREATE UNIQUE INDEX idx_enriched_user_mv_user_tmp_{{ ts_nodash }} ON public.enriched_user_mv_tmp USING btree (user_id);
-- Refresh state
refresh materialized view enriched_user_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS enriched_user_mv
    RENAME TO enriched_user_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS enriched_user_mv_tmp
    RENAME TO enriched_user_mv;
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_old;
commit
;

-- Cleanup orphaned functions left by previous runs (scheduled or manual).
-- The function still backing the freshly renamed materialized view is
-- automatically preserved: DROP FUNCTION without CASCADE fails while a
-- dependent object exists, so it is simply skipped.
create or replace function cleanup_get_enriched_user_functions_{{ ts_nodash }} ()
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
          AND p.proname ~ '^get_enriched_user_[0-9]{14}$'
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

select cleanup_get_enriched_user_functions_{{ ts_nodash }} ()
;
drop function cleanup_get_enriched_user_functions_{{ ts_nodash }} ()
;
