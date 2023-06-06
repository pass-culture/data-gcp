DROP FUNCTION IF EXISTS get_recommendable_items_raw_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_items_raw_{{ ts_nodash  }}()
RETURNS TABLE (   
                item_id varchar,
                category VARCHAR,
                subcategory_id VARCHAR,
                search_group_name VARCHAR,
                is_numerical BOOLEAN,
                is_national BOOLEAN,
                is_geolocated BOOLEAN,
                offer_is_duo BOOLEAN,
                offer_type_domain VARCHAR,
                offer_type_label VARCHAR,
                booking_number INTEGER,
                is_underage_recommendable BOOLEAN
                ) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT *
    FROM public.recommendable_items_raw;
END;
$body$
LANGUAGE plpgsql;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS recommendable_items_raw_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_items_raw_mv_tmp AS
SELECT * FROM get_recommendable_items_raw_{{ ts_nodash  }}()
WITH NO DATA;


CREATE INDEX IF NOT EXISTS item_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_items_raw_mv_tmp(item_id);

CREATE INDEX IF NOT EXISTS category_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_items_raw_mv_tmp(category);

CREATE INDEX IF NOT EXISTS subcategory_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_items_raw_mv_tmp(subcategory_id);

CREATE INDEX IF NOT EXISTS search_group_name_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_items_raw_mv_tmp(search_group_name);

-- Refresh state
REFRESH MATERIALIZED VIEW recommendable_items_raw_mv_tmp;
