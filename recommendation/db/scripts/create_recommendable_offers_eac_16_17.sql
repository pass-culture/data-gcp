/* Function to get all recommendable offers ids for EAC 16 and 17. */
DROP FUNCTION IF EXISTS get_recommendable_offers_eac_16_17 CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_eac_16_17()
RETURNS TABLE (offer_id varchar,
                product_id varchar,
                venue_id varchar,
                subcategory_id VARCHAR,
                search_group_name VARCHAR,
                category VARCHAR,
                name VARCHAR,
                url VARCHAR,
                is_national BOOLEAN,
                offer_creation_date TIMESTAMP,
                stock_beginning_date TIMESTAMP,
                stock_price REAL,
                booking_number BIGINT,
                item_id text) AS
$body$
BEGIN
    RETURN QUERY
    SELECT * FROM public.recommendable_offers_data
    where is_underage_recommendable 
    and stock_price < 30;
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_16_17;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_eac_16_17 AS
SELECT * FROM get_recommendable_offers_eac_16_17()
WITH NO DATA;



/* Populating the materialized view. */
  CREATE UNIQUE INDEX idx_offer_recommendable_16_17_id ON public.recommendable_offers_eac_16_17 USING btree (offer_id)
  CREATE INDEX idx_recommendable_offers_eac_16_17_venue_id ON public.recommendable_offers_eac_16_17 (venue_id);
  REFRESH MATERIALIZED VIEW recommendable_offers_eac_16_17;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
