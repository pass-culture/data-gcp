drop function if exists get_enriched_user_{{ ts_nodash }}
cascade
;
create or replace function get_enriched_user_{{ ts_nodash }} ()
returns
    table(
        user_id varchar,
        user_deposit_creation_date timestamp,
        user_birth_date timestamp,
        user_department_code varchar,
        user_deposit_initial_amount real,
        user_theoretical_remaining_credit real,
        booking_cnt integer,
        consult_offer integer,
        has_added_offer_to_favorites integer,
        user_subscription_latitude double precision,
        user_subscription_longitude double precision
    )
as
    $body$
BEGIN
    RETURN QUERY
    -- Select columns explicitly to avoid any issues with column order changes in the future
    SELECT
        eu.user_id,
        eu.user_deposit_creation_date,
        eu.user_birth_date,
        eu.user_department_code,
        eu.user_deposit_initial_amount,
        eu.user_theoretical_remaining_credit,
        eu.booking_cnt,
        eu.consult_offer,
        eu.has_added_offer_to_favorites,
        eu.user_subscription_latitude::double precision,
        eu.user_subscription_longitude::double precision
    FROM public.enriched_user eu;
END;
$body$
language plpgsql
;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_mv_tmp AS
SELECT * FROM get_enriched_user_{{ ts_nodash  }}()
WITH NO DATA;


CREATE UNIQUE INDEX idx_enriched_user_mv_user_tmp_{{ ts_nodash }} ON public.enriched_user_mv_tmp USING btree (user_id);
-- Refresh state
refresh materialized view enriched_user_mv_tmp
;

-- Move tmp to final Materialized view in a transaction
-- This is to avoid any downtime in case of a failure
begin
;
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS enriched_user_mv
    RENAME TO enriched_user_mv_old;
ALTER MATERIALIZED VIEW IF EXISTS enriched_user_mv_tmp
    RENAME TO enriched_user_mv;
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_old;
commit
;
