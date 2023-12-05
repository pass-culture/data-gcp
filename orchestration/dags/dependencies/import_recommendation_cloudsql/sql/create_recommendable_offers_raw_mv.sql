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
                gtl_id VARCHAR,
                gtl_l1 VARCHAR,
                gtl_l2 VARCHAR,
                gtl_l3 VARCHAR,
                gtl_l4 VARCHAR,
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
                total_offers INTEGER,
                is_underage_recommendable BOOLEAN,
                is_sensitive BOOLEAN,
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
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_raw_mv_tmp AS
SELECT * FROM get_recommendable_offers_raw_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_recommendable_offers_raw_mv_tmp_{{ ts_nodash  }} 
ON public.recommendable_offers_raw_mv_tmp 
USING btree (is_geolocated,item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS offer_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp(offer_id);

CREATE INDEX IF NOT EXISTS item_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp(item_id);

CREATE INDEX IF NOT EXISTS offer_item_idx_recommendable_offers_raw_mv_tmp_{{ ts_nodash  }} 
ON public.recommendable_offers_raw_mv_tmp 
USING btree (item_id,offer_id);

CREATE INDEX IF NOT EXISTS venue_geo_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp            
USING gist(venue_geo);


-- Refresh state
REFRESH MATERIALIZED VIEW recommendable_offers_raw_mv_tmp;
