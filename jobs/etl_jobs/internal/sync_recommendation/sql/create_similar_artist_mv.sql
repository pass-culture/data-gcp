drop function if exists get_similar_artist_{{ ts_nodash }}
cascade
;
create or replace function get_similar_artist_{{ ts_nodash }} ()
returns table(artist_id varchar, similar_artists_json varchar)
as $body$
BEGIN
    RETURN QUERY
    SELECT
        similar_artist.artist_id::varchar as artist_id,
        similar_artist.similar_artists_json::varchar as similar_artists_json
    FROM public.similar_artist similar_artist
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS similar_artist_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS similar_artist_mv_tmp AS
SELECT * FROM get_similar_artist_{{ ts_nodash  }}()
WITH NO DATA;

-- Create indexes
CREATE UNIQUE INDEX idx_similar_artist_mv_{{ ts_nodash }} ON public.similar_artist_mv_tmp USING btree (artist_id);

-- Refresh state
refresh materialized view similar_artist_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS similar_artist_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS similar_artist_mv
    RENAME TO similar_artist_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS similar_artist_mv_tmp
    RENAME TO similar_artist_mv;
DROP MATERIALIZED VIEW IF EXISTS similar_artist_mv_old;
commit
;
