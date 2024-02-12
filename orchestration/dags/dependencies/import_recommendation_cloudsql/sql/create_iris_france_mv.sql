create or replace function cast_to_int(text, integer) returns integer as $$
begin
    return cast($1 as integer);
exception
    when invalid_text_representation then
        return $2;
end;
$$ language plpgsql immutable;

DROP FUNCTION IF EXISTS get_iris_france_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_iris_france_{{ ts_nodash  }}()
RETURNS TABLE (   
            id int,
            iriscode int,
            centroid GEOGRAPHY,
            shape GEOMETRY
) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT 
    irf.id::int as id,
    cast_to_int(irf."irisCode",0) as iriscode,
    irf.centroid as centroid,
    ST_SetSRID(irf.shape::geometry, 0) as shape
    FROM public.iris_france irf;
END;
$body$
LANGUAGE plpgsql;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS iris_france_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS iris_france_mv_tmp AS
SELECT * FROM get_iris_france_{{ ts_nodash  }}()
WITH NO DATA;



CREATE INDEX idx_iris_france_centroid_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING gist (centroid);

CREATE INDEX idx_iris_france_shape_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING gist (shape);

CREATE UNIQUE INDEX iris_france_pkey_tmp_{{ ts_nodash }} ON public.iris_france_mv_tmp USING btree (id);
-- Refresh state
REFRESH MATERIALIZED VIEW iris_france_mv_tmp;
