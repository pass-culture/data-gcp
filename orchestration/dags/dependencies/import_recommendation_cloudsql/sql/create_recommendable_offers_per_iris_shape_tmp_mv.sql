DROP FUNCTION IF EXISTS get_recommendable_offers_per_iris_shape_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_per_iris_shape_{{ ts_nodash  }}()
RETURNS TABLE (   
                item_id varchar,
                offer_id varchar,
                iris_id varchar,
                venue_distance_to_iris NUMERIC,
                is_geolocated BOOLEAN,
                venue_latitude DECIMAL, 
                venue_longitude DECIMAL,
                venue_geo GEOGRAPHY,
                unique_id VARCHAR
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT
        ro.item_id,
        ro.offer_id,
        ro.iris_id,
        ro.venue_distance_to_iris,
        ro.is_geolocated,
        ro.venue_latitude,
        ro.venue_longitude,
        ST_MakePoint(ro.venue_longitude, ro.venue_latitude)::geography as venue_geo,
        ro.unique_id
    FROM public.recommendable_offers_per_iris_shape ro;
END;
$body$
LANGUAGE plpgsql;




DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_per_iris_shape_mv_tmp AS
SELECT * FROM get_recommendable_offers_per_iris_shape_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_recommendable_offers_per_iris_tmp_{{ ts_nodash  }} 
ON public.recommendable_offers_per_iris_shape_mv_tmp 
USING btree (is_geolocated,iris_id,item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS venue_geo_idx_offer_recommendable_offers_per_iris_tmp_{{ ts_nodash  }}
ON public.recommendable_offers_per_iris_shape_mv_tmp            
USING gist(venue_geo);

CREATE INDEX IF NOT EXISTS idx_offer_recommendable_offers_per_iris_tmp_{{ ts_nodash  }}
ON public.recommendable_offers_per_iris_shape_mv_tmp(offer_id);

CREATE INDEX IF NOT EXISTS item_offer_idx_recommendable_offers_per_iris_tmp_{{ ts_nodash  }} 
ON public.recommendable_offers_per_iris_shape_mv_tmp 
USING btree (item_id,offer_id);

REFRESH MATERIALIZED VIEW recommendable_offers_per_iris_shape_mv_tmp;

