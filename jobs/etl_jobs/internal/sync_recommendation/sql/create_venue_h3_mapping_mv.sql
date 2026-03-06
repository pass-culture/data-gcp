DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS venue_h3_mapping_mv_tmp AS
SELECT
    venue_id,
    latitude,
    longitude,
    h3_res3,
    h3_res4,
    h3_res5,
    h3_res6
FROM public.venue_h3_mapping
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_venue_h3_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(venue_id);
CREATE INDEX IF NOT EXISTS idx_venue_h3_res3_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(h3_res3);

CREATE INDEX IF NOT EXISTS idx_venue_h3_res4_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(h3_res4);

CREATE INDEX IF NOT EXISTS idx_venue_h3_res5_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(h3_res5);

CREATE INDEX IF NOT EXISTS idx_venue_h3_res6_{{ ts_nodash }}
ON public.venue_h3_mapping_mv_tmp(h3_res6);

REFRESH MATERIALIZED VIEW venue_h3_mapping_mv_tmp;

BEGIN;
DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv RENAME TO venue_h3_mapping_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_tmp RENAME TO venue_h3_mapping_mv;
DROP MATERIALIZED VIEW IF EXISTS venue_h3_mapping_mv_old;
COMMIT;