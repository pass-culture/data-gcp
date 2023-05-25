

/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers_per_iris_shape CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers_per_iris_shape()
RETURNS TABLE (   
                item_id varchar,
                offer_id varchar,
                iris_id varchar,
                venue_id varchar,
                venue_distance_to_iris NUMERIC,
                venue_distance_to_iris_bucket VARCHAR,
                venue_latitude DECIMAL, 
                venue_longitude DECIMAL,
                unique_id VARCHAR,
                venue_geo GEOGRAPHY
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT *, ST_MakePoint(ro.venue_longitude, ro.venue_latitude)::geography as venue_geo
    FROM public.recommendable_offers_per_iris_shape ro;
END;
$body$
LANGUAGE plpgsql;


CREATE UNIQUE INDEX IF NOT EXISTS  idx_offer_recommendable_mv
ON public.recommendable_offers_per_iris_shape_mv 
USING btree (is_geolocated,iris_id,venue_distance_to_iris_bucket,item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS  geoloc_idx_offer_recommendable_mv
ON public.recommendable_offers_per_iris_shape_mv 
USING btree (is_geolocated,iris_id,);


CREATE INDEX IF NOT EXISTS venue_geo_offer_recommendable_mv
ON public.recommendable_offers_per_iris_shape_mv          
USING gist(venue_geo);

CREATE INDEX IF NOT EXISTS offer_item_recommendable_mv
ON public.recommendable_offers_per_iris_shape_mv           
USING btree (offer_id,unique_id);


/* Creation of the materialized view. */

DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_per_iris_shape_mv AS
SELECT *
FROM get_recommendable_offers_per_iris_shape()
WITH NO DATA;




/* Populating the materialized view. */
REFRESH MATERIALIZED VIEW recommendable_offers_per_iris_shape_mv;

/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers_per_iris_shape_mv;
/* 09/11/20 : count was 26796. */
