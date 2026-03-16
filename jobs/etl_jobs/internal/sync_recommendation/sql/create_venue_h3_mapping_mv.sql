drop function if exists get_venue_h3_mapping_{{ ts_nodash }}
cascade
;
create or replace function get_venue_h3_mapping_{{ ts_nodash }} ()
returns
    table(
        venue_id integer,
        latitude decimal,
        longitude decimal,
        venue_geo geography,
        h3_res5 varchar
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT
        v.venue_id::integer,
        v.latitude::decimal,
        v.longitude::decimal,
        ST_MakePoint(v.longitude, v.latitude)::geography AS venue_geo,
        v.h3_res5::varchar
    FROM public.venue_h3_mapping v;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS venue_h3_mapping_mv_tmp AS
SELECT * FROM get_venue_h3_mapping_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_venue_h3_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(venue_id);

CREATE INDEX IF NOT EXISTS venue_geo_idx_venue_h3_{{ ts_nodash  }}
ON public.venue_h3_mapping_mv_tmp USING GIST (venue_geo);

CREATE INDEX IF NOT EXISTS idx_venue_h3_res5_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(h3_res5);

-- Refresh state
refresh materialized view venue_h3_mapping_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv RENAME TO venue_h3_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_tmp RENAME TO venue_h3_mapping_mv;
DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_old;
commit
;
