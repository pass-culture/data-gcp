/* Create the function to fetch the non recommendable offers.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_number_of_bookings_per_user CASCADE;
CREATE OR REPLACE FUNCTION get_number_of_bookings_per_user()
RETURNS TABLE (
    user_id varchar,
    bookings_count bigint) AS
$body$
BEGIN
    RETURN QUERY
    SELECT b.user_id, COUNT(*) AS bookings_count
    FROM public.booking b
    GROUP BY b.user_id;
END;
$body$
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS number_of_bookings_per_user;
CREATE MATERIALIZED VIEW IF NOT EXISTS number_of_bookings_per_user
AS
    SELECT * from get_number_of_bookings_per_user()
WITH NO DATA;


REFRESH MATERIALIZED VIEW number_of_bookings_per_user;


!-- Creating an index for faster queries.
CREATE INDEX IF NOT EXISTS idx_number_of_bookings_per_user ON public.number_of_bookings_per_user USING btree ("user_id");
