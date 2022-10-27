/* Create the function to fetch the iris_venues from the source table.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_iris_venues_150km CASCADE;
CREATE OR REPLACE FUNCTION get_iris_venues_150km()
RETURNS TABLE (iris_id varchar,
               venue_id varchar,
               venue_longitude decimal,
               venue_latitude decimal) AS
$body$
BEGIN
    RETURN QUERY
    SELECT DISTINCT v."irisId", v."venueId",v."venue_longitude",v."venue_latitude"
    FROM public.iris_venues_150km v;
END;
$body$
LANGUAGE plpgsql;


CREATE MATERIALIZED VIEW IF NOT EXISTS iris_venues_150km_mv
AS
    SELECT * from get_iris_venues_150km()
WITH NO DATA;

CREATE UNIQUE INDEX idx_iris_venues_150km_mv_irisid ON public.iris_venues_150km_mv USING btree (iris_id,venue_id);
REFRESH MATERIALIZED VIEW iris_venues_150km_mv;


/*
NB: REFRESH CONCURRENTLY is slower than a normal refresh : 1 minute vs 2 minutes
*/
