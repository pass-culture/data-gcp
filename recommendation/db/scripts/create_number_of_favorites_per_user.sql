/* Create the function to fetch the non recommendable offers.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_number_of_favorites_per_user CASCADE;
CREATE OR REPLACE FUNCTION get_number_of_favorites_per_user()
RETURNS TABLE (
    user_id varchar,
    favorites_count bigint) AS
$body$
BEGIN
    RETURN QUERY
    SELECT fe.user_id, COUNT(*) AS favorites_count
    FROM public.firebase_events fe
    WHERE fe.event_name='HasAddedOfferToFavorites'
    GROUP BY fe.user_id;
END;
$body$
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS number_of_favorites_per_user;
CREATE MATERIALIZED VIEW IF NOT EXISTS number_of_favorites_per_user
AS
    SELECT * from get_number_of_favorites_per_user()
WITH NO DATA;


REFRESH MATERIALIZED VIEW number_of_favorites_per_user;


!-- Creating an index for faster queries.
CREATE INDEX IF NOT EXISTS idx_number_of_favorites_per_user ON public.number_of_favorites_per_user USING btree ("user_id");
