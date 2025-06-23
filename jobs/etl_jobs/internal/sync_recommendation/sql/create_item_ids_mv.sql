drop function if exists get_item_ids_{{ ts_nodash }}
cascade
;
create or replace function get_item_ids_{{ ts_nodash }} ()
returns
    table(
        offer_id varchar,
        item_id varchar,
        booking_number int,
        is_sensitive bool,
        venue_latitude decimal,
        venue_longitude decimal
    )
as $body$
BEGIN
    RETURN QUERY
    SELECT
        ro.offer_id::varchar as offer_id,
        max(ro.item_id)::varchar as item_id,
        max(ro.booking_number)::int as booking_number,
        (max(ro.is_sensitive::int) > 0)::bool as is_sensitive,
        avg(ro.venue_latitude)::decimal as venue_latitude,
        avg(ro.venue_longitude)::decimal as venue_longitude
    FROM public.recommendable_offers_raw ro
    GROUP BY ro.offer_id ;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS item_ids_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS item_ids_mv_tmp AS
SELECT * FROM get_item_ids_{{ ts_nodash  }}()
WITH NO DATA;


CREATE UNIQUE INDEX idx_item_ids_mv_{{ ts_nodash }} ON public.item_ids_mv_tmp USING btree (offer_id);
-- Refresh state
refresh materialized view item_ids_mv_tmp
;

DROP MATERIALIZED VIEW IF EXISTS item_ids_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS item_ids_mv
    RENAME TO item_ids_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS item_ids_mv_tmp
    RENAME TO item_ids_mv;
DROP MATERIALIZED VIEW IF EXISTS item_ids_mv_old;
