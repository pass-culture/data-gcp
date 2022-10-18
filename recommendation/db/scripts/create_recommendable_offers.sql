/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers()
RETURNS TABLE (offer_id varchar,
                item_id varchar,
                product_id VARCHAR,
                venue_id varchar,
                subcategory_id VARCHAR,
                category VARCHAR,
                search_group_name VARCHAR,
                name VARCHAR,
                offer_is_duo BOOLEAN,
                movie_type VARCHAR,
                offer_type_id VARCHAR,
                offer_type_label VARCHAR,
                offer_sub_type_id VARCHAR,
                offer_sub_type_label VARCHAR,
                macro_rayon VARCHAR,
                url VARCHAR,
                is_national BOOLEAN,
                offer_creation_date TIMESTAMP,
                stock_beginning_date TIMESTAMP,
                stock_price REAL,
                booking_number INTEGER,
                is_underage_recommendable BOOLEAN) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT * from public.recommendable_offers_data ro;
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers AS
SELECT * FROM get_recommendable_offers()
WITH NO DATA;


/* Populating the materialized view. */
CREATE UNIQUE INDEX idx_offer_recommendable_id ON public.recommendable_offers USING btree (offer_id, stock_beginning_date);
REFRESH MATERIALIZED VIEW recommendable_offers;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
