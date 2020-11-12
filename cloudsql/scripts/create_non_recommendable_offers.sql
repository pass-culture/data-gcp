/* Creating the materialized view. */
CREATE MATERIALIZED VIEW IF NOT EXISTS non_recommendable_offers
AS
    SELECT DISTINCT b."userId" AS user_id, s."offerId" AS offer_id
      FROM public.booking b
INNER JOIN public.stock s ON b."stockId" = s.id
     WHERE b."isActive" = true
       AND b."isCancelled" = false;
WITH NO DATA;


REFRESH MATERIALIZED VIEW non_recommendable_offers;

/* Creating an index for faster queries. */
CREATE INDEX idx_non_recommendable_userid ON public.non_recommendable_offers USING btree ("user_id");

/* -------- */
/* The concurrently refresh is slower than the original refresh (7 seconds vs 5 seconds) */
/* -------- */

CREATE UNIQUE INDEX idx_non_recommendable_userid_offerid ON public.non_recommendable_offers (user_id, offer_id);


REFRESH MATERIALIZED CONCURRENTLY VIEW non_recommendable_offers;
