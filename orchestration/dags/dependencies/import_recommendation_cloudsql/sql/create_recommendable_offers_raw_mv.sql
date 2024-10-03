drop function if exists get_recommendable_offers_raw_{{ ts_nodash }}
cascade
;
create or replace function get_recommendable_offers_raw_{{ ts_nodash }} ()
returns
    table(
        offer_id varchar,
        item_id varchar,
        offer_creation_date timestamp,
        stock_beginning_date timestamp,
        booking_number integer,
        venue_latitude decimal,
        venue_longitude decimal,
        venue_geo geography,
        default_max_distance integer,
        unique_id varchar
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT
        ro.offer_id,
        ro.item_id,
        ro.offer_creation_date,
        ro.stock_beginning_date,
        ro.booking_number,
        ro.venue_latitude,
        ro.venue_longitude,
        ST_MakePoint(ro.venue_longitude, ro.venue_latitude)::geography as venue_geo,
        ro.default_max_distance,
        ro.unique_id
    FROM public.recommendable_offers_raw ro
    WHERE is_geolocated AND not is_sensitive ;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_raw_mv_tmp AS
SELECT * FROM get_recommendable_offers_raw_{{ ts_nodash  }}()
WITH NO DATA;


-- Create indexes
CREATE UNIQUE INDEX IF NOT EXISTS unique_idx_recommendable_offers_raw_mv_tmp_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp
USING btree (item_id,offer_id,unique_id);

CREATE INDEX IF NOT EXISTS offer_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp(offer_id);

CREATE INDEX IF NOT EXISTS item_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp(item_id);

CREATE INDEX IF NOT EXISTS venue_geo_idx_offer_recommendable_raw_{{ ts_nodash  }}
ON public.recommendable_offers_raw_mv_tmp
USING gist(venue_geo);


-- Refresh state
refresh materialized view recommendable_offers_raw_mv_tmp
;
