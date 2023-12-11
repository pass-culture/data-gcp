DROP FUNCTION IF EXISTS get_enriched_user_{{ ts_nodash  }} CASCADE;
CREATE OR REPLACE FUNCTION get_enriched_user_{{ ts_nodash  }}()
RETURNS TABLE (   
            user_id varchar,
            user_deposit_creation_date TIMESTAMP,
            user_birth_date TIMESTAMP,
            user_deposit_initial_amount REAL,
            user_theoretical_remaining_credit REAL,
            booking_cnt INTEGER,
            consult_offer INTEGER,
            has_added_offer_to_favorites INTEGER
) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT *
    FROM public.enriched_user;
END;
$body$
LANGUAGE plpgsql;


-- Create tmp Materialized view
DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv_tmp;
CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_mv_tmp AS
SELECT * FROM get_enriched_user_{{ ts_nodash  }}()
WITH NO DATA;



CREATE UNIQUE INDEX idx_enriched_user_mv_user_tmp_{{ ts_nodash }} ON public.enriched_user_mv_tmp USING btree (user_id);
-- Refresh state
REFRESH MATERIALIZED VIEW enriched_user_mv_tmp;
