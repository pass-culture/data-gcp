
/* Creating indexes to increase the creation of the materialized view speed. */
CREATE INDEX idx_stock_id                ON public.stock     USING btree (id);
CREATE INDEX idx_stock_offerid           ON public.stock     USING btree ("offerId");

CREATE INDEX idx_booking_stockid         ON public.booking   USING btree ("stockId");

CREATE INDEX idx_mediation_offerid       ON public.mediation USING btree ("offerId");

CREATE INDEX idx_offer_id                ON public.offer     USING btree (id);
CREATE INDEX idx_offer_type              ON public.offer     USING btree (type);
CREATE INDEX idx_offer_venueid           ON public.offer     USING btree ("venueId");

CREATE INDEX idx_venue_id                ON public.venue     USING btree (id);
CREATE INDEX idx_venue_managingoffererid ON public.venue     USING btree ("managingOffererId");

CREATE INDEX idx_offerer_id              ON public.offerer   USING btree (id);



/* Function to check if a given offer has bookable stocks (>0 and in the future). */
CREATE OR REPLACE FUNCTION offer_has_at_least_one_bookable_stock(offer_id BIGINT)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.stock
     WHERE stock."offerId" = offer_id
       AND stock."isSoftDeleted" = FALSE
       AND (
               stock."beginningDatetime" > NOW()
            OR stock."beginningDatetime" IS NULL
            )
       AND (
               stock."bookingLimitDatetime" > NOW()
            OR stock."bookingLimitDatetime" IS NULL
            )
       AND (
               stock.quantity IS NULL
            OR (
                SELECT GREATEST(stock.quantity - COALESCE(SUM(booking.quantity), 0), 0)
                  FROM public.booking
                 WHERE booking."stockId" = stock.id
                   AND booking."isCancelled" = FALSE
               ) > 0
        );
END
$body$
LANGUAGE plpgsql;



/* Function to check if a given offer has one active mediation. */
CREATE OR REPLACE FUNCTION offer_has_at_least_one_active_mediation(offer_id BIGINT)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.mediation
     WHERE mediation."offerId" = offer_id
	   AND mediation."isActive";
END
$body$
LANGUAGE plpgsql;



/* Function to get all recommendable offers ids. */
CREATE OR REPLACE FUNCTION get_recommendable_offers()
RETURNS TABLE (id BIGINT,
               venue_id BIGINT,
               type VARCHAR,
               name VARCHAR,
               url VARCHAR,
               is_national BOOLEAN) AS
$body$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (offer.id)
            offer.id           AS id,
            offer."venueId"    AS venue_id,
            offer.type         AS type,
            offer.name         AS name,
            offer.url          AS url,
            offer."isNational" AS is_national
      FROM public.offer
      JOIN public.venue ON offer."venueId" = venue.id
      JOIN public.offerer ON offerer.id = venue."managingOffererId"
     WHERE offer."isActive" = TRUE
       AND venue."validationToken" IS NULL
       AND (EXISTS (SELECT * FROM offer_has_at_least_one_active_mediation(offer.id)))
       AND (EXISTS (SELECT * FROM offer_has_at_least_one_bookable_stock(offer.id)))
       AND offerer."isActive" = TRUE
       AND offerer."validationToken" IS NULL
       AND offer.type != 'ThingType.ACTIVATION'
       AND offer.type != 'EventType.ACTIVATION';
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers AS
SELECT * FROM get_recommendable_offers()
WITH NO DATA;



/* Populating the materialized view. */
REFRESH MATERIALIZED VIEW recommendable_offers;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
