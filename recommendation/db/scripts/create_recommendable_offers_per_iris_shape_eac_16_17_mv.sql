/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers_per_iris_shape_eac_16_17 CASCADE;

CREATE
OR REPLACE FUNCTION get_recommendable_offers_per_iris_shape_eac_16_17() RETURNS TABLE (
  item_id varchar,
  offer_id varchar,
  product_id varchar,
  category VARCHAR,
  subcategory_id VARCHAR,
  search_group_name VARCHAR,
  iris_id varchar,
  venue_id varchar,
  venue_distance_to_iris NUMERIC,
  name VARCHAR,
  is_numerical BOOLEAN,
  is_national BOOLEAN,
  is_geolocated BOOLEAN,
  offer_creation_date TIMESTAMP,
  stock_beginning_date TIMESTAMP,
  stock_price REAL,
  offer_is_duo BOOLEAN,
  movie_type VARCHAR,
  offer_type_id VARCHAR,
  offer_type_label VARCHAR,
  offer_sub_type_id VARCHAR,
  offer_sub_type_label VARCHAR,
  macro_rayon VARCHAR,
  booking_number INTEGER,
  is_underage_recommendable BOOLEAN,
  "position" VARCHAR,
  unique_id VARCHAR
) AS $body$ BEGIN RETURN QUERY
SELECT
  *
from
  public.recommendable_offers_per_iris_shape reco
where
  reco.is_underage_recommendable
  and reco.stock_price < 30;

END;

$body$ LANGUAGE plpgsql;

/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_eac_16_17_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers_per_iris_shape_eac_16_17_mv AS
SELECT
  *
FROM
  get_recommendable_offers_per_iris_shape_eac_16_17() WITH NO DATA;

/* Populating the materialized view. */
CREATE UNIQUE INDEX idx_offer_recommendable_id_eac_16_17 ON public.recommendable_offers_per_iris_shape_eac_16_17_mv USING btree (
  is_geolocated,
  iris_id,
  item_id,
  offer_id,
  unique_id
);

REFRESH MATERIALIZED VIEW recommendable_offers_per_iris_shape_eac_16_17_mv;

/* Takes about 80 secondes with the indexes.*/
/* Check that the view is populated. */
SELECT
  COUNT(*)
FROM
  recommendable_offers_per_iris_shape_eac_16_17_mv;

/* 09/11/20 : count was 26796. */