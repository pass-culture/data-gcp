CREATE EXTENSION Postgis;
CREATE TABLE IF NOT EXISTS public.recommendable_offers_raw(
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
    default_max_distance INTEGER,
    unique_id VARCHAR
);

DROP FUNCTION IF EXISTS get_recommendable_offers_raw_mv CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_raw_mv()
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
                default_max_distance INTEGER,
                unique_id VARCHAR,
                venue_geo GEOGRAPHY
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT *, ST_MakePoint(ro.venue_longitude, ro.venue_latitude)::geography as venue_geo 
    FROM public.recommendable_offers_raw ro;
END;
$body$
LANGUAGE plpgsql;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_raw_mv AS
SELECT * FROM get_recommendable_offers_raw_mv()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_recommendable_offers_raw_mv
ON public.recommendable_offers_raw_mv
USING btree (is_geolocated,item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS offer_idx_recommendable_offers_raw_mv
ON public.recommendable_offers_raw_mv(offer_id);

CREATE INDEX IF NOT EXISTS item_offer_idx_recommendable_offers_raw_mv
ON public.recommendable_offers_raw_mv
USING btree (item_id, offer_id);

CREATE INDEX IF NOT EXISTS venue_geo_idx_recommendable_offers_raw_mv
ON public.recommendable_offers_raw_mv        
USING gist(venue_geo);

-- Refresh state
REFRESH MATERIALIZED VIEW recommendable_offers_raw_mv;
