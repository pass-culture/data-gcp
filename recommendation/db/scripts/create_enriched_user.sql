/* Create the function to fetch the non recommendable offers.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_enriched_user CASCADE;

CREATE OR REPLACE FUNCTION get_enriched_user() 
RETURNS TABLE (
    user_id varchar,
    user_deposit_creation_date TIMESTAMP,
    user_birth_date TIMESTAMP,
    user_deposit_initial_amount REAL
) AS 
$body$ 
BEGIN 
    RETURN QUERY
    SELECT
        *
    FROM
        public.enriched_user;
END;
$body$ 
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS enriched_user_mv AS
SELECT
    *
from
    get_enriched_user() WITH NO DATA;

REFRESH MATERIALIZED VIEW enriched_user_mv;

! -- Creating an index for faster queries.
CREATE INDEX IF NOT EXISTS idx_enriched_user_mc ON public.enriched_user_mv USING btree ("user_id");