/* Create the function to fetch the non recommendable offers.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_qpi_answers CASCADE;

CREATE OR REPLACE FUNCTION get_qpi_answers() RETURNS TABLE (
    user_id varchar,
    subcategories varchar
) AS $body$ 
BEGIN 
    RETURN QUERY
    SELECT
        *
    FROM
        public.qpi_answers;
END;
$body$ 
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS qpi_answers_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS qpi_answers_mv AS
SELECT
    *
from
    get_qpi_answers() WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_qpi_answers_mv ON public.qpi_answers_mv USING btree (user_id, subcategories);
REFRESH MATERIALIZED VIEW qpi_answers_mv;

! -- Creating an index for faster queries.