
/* Creating indexes to increase the creation of the materialized view speed. */
CREATE INDEX idx_stock_id                ON public.stock     USING btree (stock_id);
CREATE INDEX idx_stock_offerid           ON public.stock     USING btree ("offer_id");

CREATE INDEX idx_booking_stockid         ON public.booking   USING btree ("stock_id");

CREATE INDEX idx_mediation_offerid       ON public.mediation USING btree ("offerId");

CREATE INDEX idx_offer_id                ON public.offer     USING btree (offer_id);
CREATE INDEX idx_offer_type              ON public.offer     USING btree (offer_type);
CREATE INDEX idx_offer_venueid           ON public.offer     USING btree ("venue_id");

CREATE INDEX idx_venue_id                ON public.venue     USING btree (venue_id);
CREATE INDEX idx_venue_managingoffererid ON public.venue     USING btree ("venue_managing_offerer_id");

CREATE INDEX idx_offerer_id              ON public.offerer   USING btree (offerer_id);



/* Function to check if a given offer has bookable stocks (>0 and in the future). */
DROP FUNCTION IF EXISTS offer_has_at_least_one_bookable_stock;
CREATE OR REPLACE FUNCTION offer_has_at_least_one_bookable_stock(var_offer_id varchar)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.stock
     WHERE stock."offer_id" = var_offer_id
       AND stock."stock_is_soft_deleted" = FALSE
       AND (
               stock."stock_beginning_date" > NOW()
            OR stock."stock_beginning_date" IS NULL
            )
       AND (
               stock."stock_booking_limit_date" > NOW()
            OR stock."stock_booking_limit_date" IS NULL
            )
       AND (
               stock.stock_quantity IS NULL
            OR (
                SELECT GREATEST(stock.stock_quantity - COALESCE(SUM(booking.booking_quantity), 0), 0)
                  FROM public.booking
                 WHERE booking."stock_id" = stock.stock_id
                   AND booking."booking_is_cancelled" = FALSE
               ) > 0
        );
END
$body$
LANGUAGE plpgsql;



/* Function to check if a given offer has one active mediation. */
DROP FUNCTION IF EXISTS offer_has_at_least_one_active_mediation;
CREATE OR REPLACE FUNCTION offer_has_at_least_one_active_mediation(var_offer_id varchar)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.mediation
     WHERE mediation."offerId" = var_offer_id
	   AND mediation."isActive"
	   AND mediation."thumbCount" > 0;
END
$body$
LANGUAGE plpgsql;



/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers()
RETURNS TABLE (offer_id varchar,
               venue_id varchar,
               type VARCHAR,
               name VARCHAR,
               url VARCHAR,
               is_national BOOLEAN,
               booking_number BIGINT,
               item_id text,
               department character varying) AS
$body$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (offer.offer_id)
            offer.offer_id            AS offer_id,
            offer."venue_id"          AS venue_id,
            offer.offer_type          AS type,
            offer.offer_name          AS name,
            offer.offer_url           AS url,
            offer."offer_is_national" AS is_national,
            (CASE WHEN booking_numbers.booking_number IS NOT NULL THEN booking_numbers.booking_number ELSE 0 END) AS booking_number,
            (CASE WHEN offer.offer_type in ('ThingType.LIVRE_EDITION', 'EventType.CINEMA') THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) END) AS item_id,
            venue.venue_department_code as department
      FROM public.offer
      JOIN (SELECT * FROM public.venue WHERE venue_validation_token IS NULL) venue ON offer."venue_id" = venue.venue_id
      JOIN (SELECT * FROM public.offerer WHERE offerer_validation_token IS NULL) offerer ON offerer.offerer_id = venue."venue_managing_offerer_id"
      LEFT JOIN (
            SELECT count(*) AS booking_number, stock.offer_id
            FROM public.booking
            LEFT JOIN public.stock
            ON booking.stock_id = stock.stock_id
            GROUP BY stock.offer_id
      ) booking_numbers
      ON booking_numbers.offer_id = offer.offer_id
    WHERE offer."offer_is_active" = TRUE
       AND (EXISTS (SELECT * FROM offer_has_at_least_one_active_mediation(offer.offer_id)))
       AND (EXISTS (SELECT * FROM offer_has_at_least_one_bookable_stock(offer.offer_id)))
       AND offerer."offerer_is_active" = TRUE
       AND offer."offer_validation" = 'APPROVED'
       AND offer.offer_type != 'ThingType.ACTIVATION'
       AND offer.offer_type != 'EventType.ACTIVATION';
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers AS
SELECT * FROM get_recommendable_offers()
WITH NO DATA;



/* Populating the materialized view. */
REFRESH MATERIALIZED VIEW recommendable_offers;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
