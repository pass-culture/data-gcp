/* Create the function to fetch the non recommendable offers.
We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_non_recommendable_offers CASCADE;
CREATE OR REPLACE FUNCTION get_non_recommendable_offers()
RETURNS TABLE (user_id varchar,
               offer_id varchar) AS
$body$
BEGIN
    RETURN QUERY
    SELECT DISTINCT b."user_id" AS user_id, s."offer_id" AS offer_id
      FROM public.booking b
INNER JOIN public.stock s ON b."stock_id" = s.stock_id
     WHERE b."booking_is_cancelled" = false;
END;
$body$
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers;
CREATE MATERIALIZED VIEW IF NOT EXISTS non_recommendable_offers
AS
    SELECT * from get_non_recommendable_offers()
WITH NO DATA;


REFRESH MATERIALIZED VIEW non_recommendable_offers;


/* Creating an index for faster queries.
CREATE INDEX idx_non_recommendable_userid ON public.non_recommendable_offers USING btree ("user_id");
*/

/* -------- */
/* The concurrently refresh is slower than the original refresh (7 seconds vs 5 seconds) */
/* -------- */
/*
CREATE UNIQUE INDEX idx_non_recommendable_userid_offerid ON public.non_recommendable_offers (user_id, offer_id);


REFRESH MATERIALIZED CONCURRENTLY VIEW non_recommendable_offers;

*/
INSERT INTO booking (booking_id, booking_creation_date, stock_id, booking_quantity, user_id, booking_amount, booking_is_cancelled, booking_is_used, booking_used_date, booking_cancellation_date)
VALUES ('1', '2021-03-29T19:24:38.946506', '222284', '1', '124776', '29.99', false, true, null, null)
