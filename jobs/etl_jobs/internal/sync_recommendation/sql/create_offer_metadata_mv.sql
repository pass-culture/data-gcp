drop function if exists get_offer_metadata_{{ ts_nodash }}
cascade
;
create or replace function get_offer_metadata_{{ ts_nodash }} ()
returns table(offer_id varchar, search_group_name varchar)
as $body$
BEGIN
    RETURN QUERY
    SELECT
        offer_metadata.offer_id::varchar as offer_id,
        offer_metadata.search_group_name::varchar as search_group_name
    FROM public.offer_metadata offer_metadata;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS offer_metadata_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS offer_metadata_mv_tmp AS
SELECT * FROM get_offer_metadata_{{ ts_nodash  }}()
WITH NO DATA;

-- Create indexes
CREATE UNIQUE INDEX idx_offer_metadata_mv_{{ ts_nodash }} ON public.offer_metadata_mv_tmp USING btree (offer_id);

-- Refresh state
refresh materialized view offer_metadata_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS offer_metadata_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS offer_metadata_mv
    RENAME TO offer_metadata_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS offer_metadata_mv_tmp
    RENAME TO offer_metadata_mv;
DROP MATERIALIZED VIEW IF EXISTS offer_metadata_mv_old;
commit
;
