create or replace function get_qpi_gtl_mapping_{{ ts_nodash }} ()
returns table(backend_name varchar, gtl_id varchar, gtl_type varchar)
as $body$
BEGIN
    RETURN QUERY
    SELECT
        qpi_gtl_mapping.ux_name::varchar as gtl_type,
        qpi_gtl_mapping.backend_name::varchar as backend_name,
        qpi_gtl_mapping.gtl_label::varchar as gtl_label
    FROM public.qpi_gtl_mapping qpi_gtl_mapping;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS qpi_gtl_mapping_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS qpi_gtl_mapping_mv_tmp AS
SELECT * FROM get_qpi_gtl_mapping_{{ ts_nodash  }}()
WITH NO DATA;

-- Create indexes
-- One row per (backend_name, gtl_id): a backend_name can map to several
-- gtl_ids (one per matched GTL label).
CREATE UNIQUE INDEX idx_qpi_gtl_mapping_mv_{{ ts_nodash }}
    ON public.qpi_gtl_mapping_mv_tmp USING btree (backend_name, gtl_id);

-- Refresh state
refresh materialized view qpi_gtl_mapping_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS qpi_gtl_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS qpi_gtl_mapping_mv
    RENAME TO qpi_gtl_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS qpi_gtl_mapping_mv_tmp
    RENAME TO qpi_gtl_mapping_mv;
DROP MATERIALIZED VIEW IF EXISTS qpi_gtl_mapping_mv_old;
commit
;

-- Cleanup orphaned functions left by previous runs (scheduled or manual).
-- The function still backing the freshly renamed materialized view is
-- automatically preserved: DROP FUNCTION without CASCADE fails while a
-- dependent object exists, so it is simply skipped.
create or replace function cleanup_get_qpi_gtl_mapping_functions_{{ ts_nodash }} ()
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
          AND p.proname ~ '^get_qpi_gtl_mapping_[0-9]{14}$'
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

select cleanup_get_qpi_gtl_mapping_functions_{{ ts_nodash }} ()
;
drop function cleanup_get_qpi_gtl_mapping_functions_{{ ts_nodash }} ()
;
