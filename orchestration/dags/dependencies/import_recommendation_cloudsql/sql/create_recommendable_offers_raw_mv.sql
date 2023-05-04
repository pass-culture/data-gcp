DROP FUNCTION IF EXISTS get_recommendable_offers_raw_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_raw_{{ ts_nodash  }}()
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
                unique_id VARCHAR
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT * from public.recommendable_offers_raw ro;
END;
$body$
LANGUAGE plpgsql;




DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_tmp_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_raw_tmp_mv AS
SELECT * FROM get_recommendable_offers_raw_{{ ts_nodash  }}()
WITH NO DATA;



CREATE UNIQUE INDEX IF NOT EXISTS idx_recommendable_offers_raw_tmp_mv_{{ ts_nodash  }} 
ON public.recommendable_offers_raw_tmp_mv 
USING btree (is_geolocated,item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS idx_offer_recommendable_raw
ON public.recommendable_offers_raw_tmp_mv           
USING btree (offer_id,unique_id);

CREATE INDEX loc_idx_offer_recommendable_raw 
ON public.recommendable_offers_raw_tmp_mv            
USING gist (ll_to_earth(venue_latitude, venue_longitude));

REFRESH MATERIALIZED VIEW recommendable_offers_raw_tmp_mv;

