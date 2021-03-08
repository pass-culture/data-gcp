-- compare booking volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.booking'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_booking_from_csql,
  (
    SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.enriched_booking_data`
    WHERE booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_booking_from_big_query_analytics,
  (
    SELECT count(booking_id) FROM `passculture-data-prod.clean_prod.applicative_database_booking`
    WHERE booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_booking_from_big_query_clean;



-- compare offer volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.offer'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_offer_from_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.enriched_offer_data`
    WHERE offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_offer_from_big_query,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.clean_prod.applicative_database_offer`
    WHERE offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_offer_from_big_query_clean;


-- compare venue volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.venue'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_venue_from_csql,
  (
    SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.enriched_venue_data`
    WHERE first_offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_venue_from_big_query,
  (
    SELECT count(venue_id) FROM `passculture-data-prod.clean_prod.applicative_database_venue`
    WHERE venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_venue_from_big_query_clean;



-- compare offerer volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.offerer'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_offerer_from_csql,
  (
    SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.enriched_offerer_data`
    WHERE offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_offerer_from_big_query,
  (
    SELECT count(offerer_id) FROM `passculture-data-prod.clean_prod.applicative_database_offerer`
    WHERE offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_offerer_from_big_query_clean;


-- compare stock volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.stock'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_stock_from_csql,
  (
    SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.enriched_stock_data`
    WHERE stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_stock_from_big_query,
  (
    SELECT count(stock_id) FROM `passculture-data-prod.clean_prod.applicative_database_stock`
    WHERE stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_stock_from_big_query_clean;


-- compare user volumetry
SELECT (
  SELECT count(id) FROM EXTERNAL_QUERY(
              'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
              ' SELECT CAST("id" AS varchar(255)), "dateCreated" as date_created FROM public.user'
          ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_user_from_csql,
  (
    SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.enriched_user_data`
    WHERE activation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_enriched_user_from_big_query,
  (
    SELECT count(user_id) FROM `passculture-data-prod.clean_prod.applicative_database_user`
    WHERE user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
  ) as count_user_from_big_query_clean;



-- compare enriched_offer_data max date passculture-metier-ehp
SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" FROM public.offer'
    );

-- compare enriched_offer_data max date passculture-data-prod
WITH last_offer_date_csql as (
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" FROM public.offer'
    )
),
last_enriched_offer_date_bq_analytics as (
  SELECT MAX(offer_creation_date) FROM `passculture-data-prod.analytics_prod.enriched_offer_data`
),
  last_offer_date_bq_clean as (
  SELECT MAX(offer_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_offer`
)

SELECT (
      SELECT * FROM last_offer_date_csql
) as csql_date,
(
    SELECT * FROM last_enriched_offer_date_bq_analytics
) as bq_date_analytics,
(
    SELECT * FROM last_offer_date_bq_clean
) as bq_date_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_offer_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_enriched_offer_date_bq_analytics) as TIMESTAMP), SECOND)
) as timestamp_diff_analytics


-- compare enriched_offerer_data max date
WITH last_offerer_date_csql as (
  SELECT MAX(date_created) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" as date_created FROM public.offerer'
    ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_enriched_offerer_date_bq_analytics as (
  SELECT MAX(offerer_creation_date) FROM `passculture-data-prod.analytics_prod.enriched_offerer_data`
  WHERE offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_offerer_date_bq_clean as (
  SELECT MAX(offerer_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_offerer`
  WHERE offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
)

SELECT (
    SELECT * FROM last_offerer_date_csql
) as csql_date,
(
    SELECT * FROM last_enriched_offerer_date_bq_analytics
) as bq_date_analytics,
(
    SELECT * FROM last_offerer_date_bq_clean
) as bq_date_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offerer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_offerer_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offerer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_enriched_offerer_date_bq_analytics) as TIMESTAMP), SECOND)
) as timestamp_diff_analytics


-- compare enriched_stock_data max date
WITH last_stock_date_csql as (
  SELECT MAX(date_created) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" as date_created FROM public.stock'
    ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_enriched_stock_date_bq_analytics as (
  SELECT MAX(stock_creation_date) FROM `passculture-data-prod.analytics_prod.enriched_stock_data`
  WHERE stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_stock_date_bq_clean as (
  SELECT MAX(stock_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_stock`
  WHERE stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
)

SELECT (
    SELECT * FROM last_stock_date_csql
) as csql_date,
(
    SELECT * FROM last_enriched_stock_date_bq_analytics
) as bq_date_analytics,
(
    SELECT * FROM last_stock_date_bq_clean
) as bq_date_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_stock_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_stock_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_stock_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_enriched_stock_date_bq_analytics) as TIMESTAMP), SECOND)
) as timestamp_diff_analytics


-- compare enriched_venue_data max date
WITH last_venue_date_csql as (
	SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" FROM public.venue'
    )
),
	last_enriched_venue_date_bq_analytics as (
	SELECT MAX(first_offer_creation_date) FROM `passculture-data-prod.analytics_prod.enriched_venue_data`
),
	last_venue_date_bq_clean as (
	SELECT MAX(venue_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_venue`
)

SELECT (
    SELECT * FROM last_venue_date_csql
) as csql_date,
(
    SELECT * FROM last_enriched_venue_date_bq_analytics
) as bq_date_analytics,
(
    SELECT * FROM last_venue_date_bq_clean
) as bq_date_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_venue_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_venue_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_venue_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_enriched_venue_date_bq_analytics) as TIMESTAMP), SECOND)
) as timestamp_diff_analytics


-- compare user data max date
WITH last_user_date_csql as (
  SELECT MAX(date_created) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" as date_created FROM public.user'
    ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_user_date_bq_clean as (
  SELECT MAX(user_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_user`
  WHERE user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
)

SELECT (
    SELECT * FROM last_user_date_csql
) as last_user_date_csql,
(
    SELECT * FROM last_user_date_bq_clean
) as last_user_date_bq_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_user_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_user_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean


-- compare enriched_booking_data max date
WITH last_booking_date_csql as (
  SELECT MAX(date_created) FROM EXTERNAL_QUERY(
      'passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
      'SELECT "dateCreated" as date_created FROM public.booking'
    ) WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_enriched_booking_date_bq_analytics as (
  SELECT MAX(booking_creation_date) FROM `passculture-data-prod.analytics_prod.enriched_booking_data`
  WHERE booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
),
  last_booking_date_bq_clean as (
  SELECT MAX(booking_creation_date) FROM `passculture-data-prod.clean_prod.applicative_database_booking`
  WHERE booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
)

SELECT (
    SELECT * FROM last_booking_date_csql
) as csql_date,
(
    SELECT * FROM last_enriched_booking_date_bq_analytics
) as bq_date_analytics,
(
    SELECT * FROM last_booking_date_bq_clean
) as bq_date_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_booking_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_booking_date_bq_clean) as TIMESTAMP), SECOND)
) as timestamp_diff_clean,
(
    SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_booking_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_enriched_booking_date_bq_analytics) as TIMESTAMP), SECOND)
) as timestamp_diff_analytics



-- compare booking null values in csql and bigquery
SELECT
(
  SELECT "booking_id"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_id_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_id IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_dateCreated_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_creation_date IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_dateCreated_big_query
UNION ALL
SELECT
(
  SELECT "stock_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "stockId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stockId_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE stock_id IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_quantity"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "quantity" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_quantity_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_quantity IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_quantity_big_query
UNION ALL
SELECT
(
  SELECT "user_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "userId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_userId_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE user_id IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_amount"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "amount" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_amount_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_amount IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_amount_big_query
UNION ALL
SELECT
(
  SELECT "booking_is_cancelled"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "isCancelled" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_isCancelled_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_is_cancelled IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_is_cancelled_big_query
UNION ALL
SELECT
(
  SELECT "booking_is_used"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "isUsed" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_isUsed_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_is_used IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_is_used_big_query
UNION ALL
SELECT
(
  SELECT "booking_used_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "dateUsed" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_dateUsed_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_used_date IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_dateUsed_big_query
UNION ALL
SELECT
(
  SELECT "booking_cancellation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.booking WHERE "cancellationDate" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_cancellationDate_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  WHERE booking_cancellation_date IS NULL
  AND booking_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_cancellationDate_big_query




-- compare offerer null values in csql and bigquery
SELECT
(
  SELECT "offerer_is_active"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
   'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "isActive" IS NULL')
   WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_is_active_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_is_active IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_is_active_big_query
UNION ALL
SELECT
(
  SELECT "offerer_thumb_count"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
   'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "thumbCount" IS NULL')
   WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_thumb_count_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_thumb_count IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_thumb_count_big_query
UNION ALL
SELECT
(
  SELECT "offerer_id_at_providers"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "idAtProviders" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_id_at_providers_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_id_at_providers IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "offerer_modified_at_last_provider_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "dateModifiedAtLastProvider" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_modified_at_last_provider_date_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_modified_at_last_provider_date IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "offerer_address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "address" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_address_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_address IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_address_big_query
UNION ALL
SELECT
(
  SELECT "offerer_postal_code"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "postalCode" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_postal_code_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_postal_code IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_postal_code_big_query
UNION ALL
SELECT
(
  SELECT "offerer_city"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "city" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_city_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_city IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_city_big_query
UNION ALL
SELECT
(
  SELECT "offerer_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_id_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_id IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_id_big_query
UNION ALL
SELECT
(
  SELECT "offerer_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_creation_date_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_creation_date IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "offerer_name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "name" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_name_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_name IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_name_big_query
UNION ALL
SELECT
(
  SELECT "offerer_siren"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "siren" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_siren_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_siren IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_siren_big_query
UNION ALL
SELECT
(
  SELECT "offerer_last_provider_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "lastProviderId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_last_provider_id_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_last_provider_id IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_last_provider_id_big_query
UNION ALL
SELECT
(
  SELECT "offerer_fields_updated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offerer WHERE "fieldsUpdated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offerer_fields_updated_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  WHERE offerer_fields_updated IS NULL
  AND offerer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offerer_fields_updated_big_query




-- compare offer null values in csql and bigquery
SELECT
(
  SELECT "offer_id_at_providers"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "idAtProviders" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_id_at_providers_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_id_at_providers IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "offer_modified_at_last_provider_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "dateModifiedAtLastProvider" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_modified_at_last_provider_date_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_modified_at_last_provider_date IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "offer_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_id_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_id IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_id_big_query
UNION ALL
SELECT
(
  SELECT "offer_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_creation_date_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_creation_date IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "offer_product_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "productId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_product_id_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_product_id IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_product_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "venueId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_id_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE venue_id IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_id_big_query
UNION ALL
SELECT
(
  SELECT "offer_last_provider_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "lastProviderId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_last_provider_id_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_last_provider_id IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_last_provider_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_email"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "bookingEmail" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_booking_email_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE booking_email IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_booking_email_big_query
UNION ALL
SELECT
(
  SELECT "offer_address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "isActive" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_is_active_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_is_active IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_is_active_big_query
UNION ALL
SELECT
(
  SELECT "offer_type"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "type" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_type_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_type IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_type_big_query
UNION ALL
SELECT
(
  SELECT "offer_name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "name" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_name_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_name IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_name_big_query
UNION ALL
SELECT
(
  SELECT "offer_description"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "description" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_description_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_description IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_description_big_query
UNION ALL
SELECT
(
  SELECT "offer_age_min"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "ageMin" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_age_min_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_age_min IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_age_min_big_query
UNION ALL
SELECT
(
  SELECT "offer_age_max"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "ageMax" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_age_max_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_age_max IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_age_max_big_query
UNION ALL
SELECT
(
  SELECT "offer_url"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "url" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_url_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_url IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_url_big_query
UNION ALL
SELECT
(
  SELECT "offer_duration_minutes"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "durationMinutes" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_duration_minutes_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_duration_minutes IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_duration_minutes_big_query
UNION ALL
SELECT
(
  SELECT "offer_is_national"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "isNational" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_is_national_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_is_national IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_is_national_big_query
UNION ALL
SELECT
(
  SELECT "offer_extra_data"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "extraData" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_extra_data_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_extra_data IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_extra_data_big_query
UNION ALL
SELECT
(
  SELECT "offer_is_duo"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "isDuo" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_is_duo_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_is_duo IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_is_duo_big_query
UNION ALL
SELECT
(
  SELECT "offer_fields_updated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "fieldsUpdated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_fields_updated_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_fields_updated IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_fields_updated_big_query
UNION ALL
SELECT
(
  SELECT "offer_withdrawal_details"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.offer WHERE "withdrawalDetails" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_withdrawal_details_csql,
(
  SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  WHERE offer_withdrawal_details IS NULL
  AND offer_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_withdrawal_details_big_query



-- compare venue null values in csql and bigquery
SELECT
(
  SELECT "venue_thumb_count"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "thumbCount" IS NULL')
   WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_thumb_count_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_thumb_count IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_thumb_count_big_query
UNION ALL
SELECT
(
  SELECT "venue_id_at_providers"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "idAtProviders" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_id_at_providers_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_id_at_providers IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "venue_modified_at_last_provider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "dateModifiedAtLastProvider" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_modified_at_last_provider_date_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_modified_at_last_provider IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "venue_address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "address" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_address_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_address IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_address_big_query
UNION ALL
SELECT
(
  SELECT "venue_postal_code"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "postalCode" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_postal_code_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_postal_code IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_postal_code_big_query
UNION ALL
SELECT
(
  SELECT "venue_city"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "city" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_city_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_city IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_city_big_query
UNION ALL
SELECT
(
  SELECT "venue_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_id IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "name" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_name_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_name IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_name_big_query
UNION ALL
SELECT
(
  SELECT "venue_siret"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "siret" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_siret_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_siret IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_siret_big_query
UNION ALL
SELECT
(
  SELECT "venue_department_code"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "departementCode" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_department_code_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_department_code IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_department_code_big_query
UNION ALL
SELECT
(
  SELECT "venue_latitude"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "latitude" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_latitude_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_latitude IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_latitude_big_query
UNION ALL
SELECT
(
  SELECT "venue_longitude"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "longitude" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_longitude_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_longitude IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_longitude_big_query
UNION ALL
SELECT
(
  SELECT "venue_managing_offerer_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "managingOffererId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_managing_offerer_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_managing_offerer_id IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_managing_offerer_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_booking_email"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "bookingEmail" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_booking_email_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_booking_email IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_booking_email_big_query
UNION ALL
SELECT
(
  SELECT "venue_is_virtual"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "isVirtual" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_is_virtual_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_is_virtual IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_is_virtual_big_query
UNION ALL
SELECT
(
  SELECT "venue_comment"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "comment" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_comment_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_comment IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_comment_big_query
UNION ALL
SELECT
(
  SELECT "venue_public_name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "publicName" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_public_name_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_public_name IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_public_name_big_query
UNION ALL
SELECT
(
  SELECT "venue_fields_updated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "fieldsUpdated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_fields_updated_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_fields_updated IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_fields_updated_big_query
UNION ALL
SELECT
(
  SELECT "venue_type_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "venueTypeId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_type_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_type_id IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_type_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_label_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "venueLabelId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_label_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_label_id IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_label_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.venue WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_venue_creation_date_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  WHERE venue_creation_date IS NULL
  AND venue_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_venue_creation_date_big_query




-- compare stock null values in csql and bigquery
SELECT
(
  SELECT "stock_id_at_providers"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "idAtProviders" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_id_at_providers_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_id_at_providers IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "stock_modified_at_last_provider_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "dateModifiedAtLastProvider" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_modified_at_last_provider_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_modified_at_last_provider_date IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id,"dateCreated" as date_created FROM public.stock WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_id_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_id IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_id_big_query
UNION ALL
SELECT
(
  SELECT "stock_modified_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "dateModified" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_modified_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_modified_date IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_modified_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_price"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "price" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_price_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_price IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_price_big_query
UNION ALL
SELECT
(
  SELECT "stock_quantity"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "quantity" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_quantity_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_quantity IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_quantity_big_query
UNION ALL
SELECT
(
  SELECT "stock_booking_limit_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "bookingLimitDatetime" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_booking_limit_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_booking_limit_date IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_booking_limit_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_last_provider_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "lastProviderId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_last_provider_id_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_last_provider_id IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_last_provider_id_big_query
UNION ALL
SELECT
(
  SELECT "offer_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "offerId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_offer_id_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE offer_id IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_offer_id_big_query
UNION ALL
SELECT
(
  SELECT "stock_is_soft_deleted"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "isSoftDeleted" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_is_soft_deleted_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_is_soft_deleted IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_is_soft_deleted_big_query
UNION ALL
SELECT
(
  SELECT "stock_beginning_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "beginningDatetime" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_beginning_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_beginning_date IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_beginning_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_creation_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_creation_date IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_fields_updated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.stock WHERE "fieldsUpdated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_stock_fields_updated_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  WHERE stock_fields_updated IS NULL
  AND stock_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_stock_fields_updated_big_query



-- compare user null values in csql and bigquery
SELECT
(
  SELECT "user_id"
)  as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "id" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_id_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_id IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "user_creation_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "dateCreated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_dateCreated_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_creation_date IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "user_department_code"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "departementCode" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_departement_code_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_department_code IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_department_code_big_query
UNION ALL
SELECT
(
  SELECT "user_is_beneficiary"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "isBeneficiary" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_is_beneficiary_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_is_beneficiary IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_is_beneficiary_big_query
UNION ALL
SELECT
(
  SELECT "user_is_admin"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "isAdmin" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_is_Admin_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_is_admin IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_is_Admin_big_query
UNION ALL
SELECT
(
  SELECT "user_reset_password_token_validity_limit"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "resetPasswordTokenValidityLimit" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_reset_psw_token_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_reset_password_token_validity_limit IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_reset_psw_token_big_query
UNION ALL
SELECT
(
  SELECT "user_postal_code"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "postalCode" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_postal_code_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_postal_code IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_postal_code_big_query
UNION ALL
SELECT
(
  SELECT "user_needs_to_fill_cultural_survey"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "needsToFillCulturalSurvey" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_needs_to_fill_cultural_survey_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_needs_to_fill_cultural_survey IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_user_needs_to_fill_cultural_survey_big_query
UNION ALL
SELECT
(
  SELECT "user_cultural_survey_id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "culturalSurveyId" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_cultural_survey_id_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_cultural_survey_id IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_cultural_survey_id_big_query
UNION ALL
SELECT
(
  SELECT "user_civility"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "civility" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_civility_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_civility IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_civility_big_query
UNION ALL
SELECT
(
  SELECT "user_activity"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "activity" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_activity_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_activity IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_activity_big_query
UNION ALL
SELECT
(
  SELECT "user_cultural_survey_filled_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "culturalSurveyFilledDate" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_cultural_survey_filled_date_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_cultural_survey_filled_date IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_cultural_survey_filled_date_big_query
UNION ALL
SELECT
(
  SELECT "user_has_seen_tutorials"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "hasSeenTutorials" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_has_seen_tutorials_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_has_seen_tutorials IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_has_seen_tutorials_big_query
UNION ALL
SELECT
(
  SELECT "user_address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "address" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_address_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_address IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_address_big_query
UNION ALL
SELECT
(
  SELECT "user_city"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "city" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_city_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_city IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_city_big_query
UNION ALL
SELECT
(
  SELECT "user_last_connection_date"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "lastConnectionDate" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_last_connection_date_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_last_connection_date IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_last_connection_date_big_query
UNION ALL
SELECT
(
  SELECT "user_is_email_validated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "isEmailValidated" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_is_email_validated_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_is_email_validated IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_is_email_validated_big_query
UNION ALL
SELECT
(
  SELECT "user_has_allowed_recommendations"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "hasAllowedRecommendations" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_has_allowed_recommendations_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_has_allowed_recommendations IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_has_allowed_recommendations_big_query
UNION ALL
SELECT
(
  SELECT "user_suspension_reason"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "suspensionReason" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_suspension_reason_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_suspension_reason IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_suspension_reason_big_query
UNION ALL
SELECT
(
  SELECT "user_is_active"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-metier-prod.europe-west1.metier-pcapi-production-connection',
  'SELECT id, "dateCreated" as date_created FROM public.user WHERE "isActive" IS NULL')
  WHERE date_created <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00")
  AS DATETIME)
) as count_null_in_user_is_active_csql,
(
  SELECT count(user_id) FROM `passculture-data-prod.analytics_prod.applicative_database_user`
  WHERE user_is_active IS NULL
  AND user_creation_date <= CAST(CONCAT(EXTRACT(DATE from DATETIME_ADD(CURRENT_DATETIME(), INTERVAL -1 DAY)), " 05:00:00") AS DATETIME)
) as count_null_in_user_is_active_big_query
