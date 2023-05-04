/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers_raw CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_raw()
RETURNS TABLE (   
                item_id varchar,
                offer_id varchar,
                product_id varchar,
                category VARCHAR,
                subcategory_id VARCHAR,
                search_group_name VARCHAR,
                venue_id varchar,
                name VARCHAR,
                is_numerical BOOLEAN,
                is_national BOOLEAN,
                is_geolocated BOOLEAN,
                offer_creation_date TIMESTAMP,
                stock_beginning_date TIMESTAMP,
                stock_price REAL,
                offer_is_duo BOOLEAN,
                offer_type_domain VARCHAR,
                offer_type_label VARCHAR,
                booking_number INTEGER,
                is_underage_recommendable BOOLEAN,
                venue_latitude DECIMAL, 
                venue_longitude DECIMAL,
                max_distance INTEGER,
                unique_id VARCHAR
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT * from public.get_recommendable_offers_raw ro;
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_raw_mv AS
SELECT * FROM get_recommendable_offers_raw()
WITH NO DATA;


/* Populating the materialized view. */
CREATE UNIQUE INDEX idx_offer_recommendable_raw ON public.recommendable_offers_raw_mv USING btree (is_geolocated,item_id,offer_id,unique_id);
CREATE INDEX ON idx_locs USING gist (ll_to_earth(venue_latitude, venue_longitude));

REFRESH MATERIALIZED VIEW recommendable_offers_raw_mv;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers_raw_mv;
/* 09/11/20 : count was 26796. */
