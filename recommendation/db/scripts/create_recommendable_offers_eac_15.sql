/* Function to get all recommendable offers ids for EAC 15. */
DROP FUNCTION IF EXISTS get_recommendable_offers_eac_15 CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_eac_15()
RETURNS TABLE (offer_id varchar,
                product_id varchar,
                venue_id varchar,
                subcategory_id VARCHAR,
                category VARCHAR,
                search_group_name VARCHAR,
                name VARCHAR,
                url VARCHAR,
                is_national BOOLEAN,
                offer_creation_date TIMESTAMP,
                stock_beginning_date TIMESTAMP,
                stock_price REAL,
                booking_number INTEGER,
                item_id VARCHAR,
                is_underage_recommendable BOOLEAN) AS
$body$
BEGIN
    RETURN QUERY
    SELECT * FROM public.recommendable_offers_data reco
    where reco.is_underage_recommendable 
    and reco.stock_price < 20;
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_15;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_eac_15 AS
SELECT * FROM get_recommendable_offers_eac_15()
WITH NO DATA;



/* Populating the materialized view. */

CREATE UNIQUE INDEX idx_offer_recommendable_15_id ON public.recommendable_offers_eac_15 USING btree (offer_id,stock_beginning_date);
REFRESH MATERIALIZED VIEW recommendable_offers_eac_15;
/* Takes about 80 secondes with the indexes.*/



/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
