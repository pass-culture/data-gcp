DROP FUNCTION IF EXISTS get_recommendable_offers_per_iris_shape_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_per_iris_shape_{{ ts_nodash  }}()
RETURNS TABLE (   
                item_id varchar,
                offer_id varchar,
                product_id varchar,
                category VARCHAR,
                subcategory_id VARCHAR,
                search_group_name VARCHAR,
                iris_id varchar,
                venue_id varchar,
                venue_distance_to_iris NUMERIC,
                venue_distance_to_iris_bucket VARCHAR,
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
                offer_type_labels VARCHAR,
                booking_number INTEGER,
                is_underage_recommendable BOOLEAN,
                "position" VARCHAR,
                venue_latitude DECIMAL, 
                venue_longitude DECIMAL,
                unique_id VARCHAR
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT * from public.recommendable_offers_per_iris_shape ro;
END;
$body$
LANGUAGE plpgsql;




DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_tmp_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_per_iris_shape_tmp_mv AS
SELECT * FROM get_recommendable_offers_per_iris_shape_{{ ts_nodash  }}()
WITH NO DATA;



CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_recommendable_mv_tmp_{{ ts_nodash  }} ON public.recommendable_offers_per_iris_shape_tmp_mv USING btree (is_geolocated,iris_id,venue_distance_to_iris_bucket,item_id,offer_id,unique_id);
REFRESH MATERIALIZED VIEW recommendable_offers_per_iris_shape_tmp_mv;

