/* Create the function to fetch the iris_venues from the source table.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
CREATE OR REPLACE FUNCTION get_iris_venues()
RETURNS TABLE (iris_id varchar,
               venue_id varchar) AS
$body$
BEGIN
    RETURN QUERY
    SELECT DISTINCT "irisId", "venueId"
      FROM public.iris_venues;
END;
$body$
LANGUAGE plpgsql;


CREATE MATERIALIZED VIEW IF NOT EXISTS iris_venues_mv
AS
    SELECT * from get_iris_venues()
WITH NO DATA;


REFRESH MATERIALIZED VIEW iris_venues_mv;

CREATE INDEX idx_iris_venues_mv_irisid ON public.iris_venues_mv USING btree (iris_id);

/*
NB: REFRESH CONCURRENTLY is slower than a normal refresh : 1 minute vs 2 minutes
*/
