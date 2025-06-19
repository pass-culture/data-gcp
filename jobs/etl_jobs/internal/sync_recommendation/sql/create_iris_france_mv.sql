create or replace function cast_to_int(text, integer)
returns integer
as $$
begin
    return cast($1 as integer);
exception
    when invalid_text_representation then
        return $2;
end;
$$
language plpgsql
immutable
;

drop function if exists get_iris_france_{{ ts_nodash }}
cascade
;
create or replace function get_iris_france_{{ ts_nodash }} ()
returns table(id int, iriscode int, centroid geography, shape geometry)
as $body$
BEGIN
    RETURN QUERY
    SELECT
    irf.id::int as id,
    cast_to_int(irf."irisCode",0) as iriscode,
    irf.centroid::geography as centroid,
    ST_SetSRID(irf.shape::geometry, 0) as shape
    FROM public.iris_france irf;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS iris_france_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS iris_france_mv_tmp AS
SELECT * FROM get_iris_france_{{ ts_nodash  }}()
WITH NO DATA;


CREATE INDEX idx_iris_france_centroid_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING gist (centroid);

CREATE INDEX idx_iris_france_shape_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING gist (shape);

CREATE UNIQUE INDEX iris_france_pkey_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING btree (id);
-- Refresh state
refresh materialized view iris_france_mv_tmp
;

-- Move tmp to final Materialized view
-- mv -> mv_old
-- mv_tmp -> mv
-- drop mv_old if exists
ALTER MATERIALIZED VIEW IF EXISTS iris_france_mv
    RENAME TO iris_france_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS iris_france_mv_tmp
    RENAME TO iris_france_mv;
DROP MATERIALIZED VIEW IF EXISTS iris_france_mv_old;
