-- compare booking volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT CAST("booking_id" AS varchar(255)) FROM public.booking'
          )
  ) as count_booking_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
  ) as count_booking_from_big_query

-- compare mediation volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT CAST("id" AS varchar(255)) FROM public.mediation'
          )
  ) as count_mediation_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation`
  ) as count_mediation_from_big_query

-- compare offer volumetry
SELECT (
    SELECT count(*) FROM EXTERNAL_QUERY(
                'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
                'SELECT CAST("offer_id" AS varchar(255)) FROM public.offer'
            )
  ) as count_offer_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
  ) as count_offer_from_big_query

-- compare offerer volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT CAST("offerer_id" AS varchar(255)) FROM public.offerer'
          )
  ) as count_offerer_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
  ) as count_offerer_from_big_query

-- compare stock volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT CAST("stock_id" AS varchar(255)) FROM public.stock'
          )
  ) as count_stock_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
  ) as count_stock_from_big_query

-- compare venue volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT CAST("venue_id" AS varchar(255)) FROM public.venue'
          )
  ) as count_venue_from_csql,
  (
    SELECT count(*) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
  ) as count_venue_from_big_query

-- compare last date created booking
WITH last_booking_date_csql as (
  SELECT MAX(booking_creation_date) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "booking_creation_date" FROM public.booking'
          )
    ),
    last_booking_date_big_query as (
        SELECT MAX(booking_creation_date) FROM `passculture-data-prod.analytics_prod.applicative_database_booking`
    )
SELECT (
        SELECT * FROM last_booking_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_booking_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_booking_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_booking_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare last date created mediation
WITH last_mediation_date_csql as (
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "dateCreated" FROM public.mediation'
          )
    ),
    last_mediation_date_big_query as (
        SELECT MAX(dateCreated) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation`
    )
SELECT (
        SELECT * FROM last_mediation_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_mediation_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_mediation_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_mediation_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare last date created offer
WITH last_offer_date_csql as (
  SELECT MAX(offer_creation_date) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "offer_creation_date" FROM public.offer'
          )
    ),
    last_offer_date_big_query as (
        SELECT MAX(offer_creation_date) FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
    )
SELECT (
        SELECT * FROM last_offer_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_offer_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_offer_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare last date created offerer
WITH last_offerer_date_csql as (
  SELECT MAX(offerer_creation_date) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "offerer_creation_date" FROM public.offerer'
          )
    ),
    last_offerer_date_big_query as (
        SELECT MAX(offerer_creation_date) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer`
    )
SELECT (
        SELECT * FROM last_offerer_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_offerer_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_offerer_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_offerer_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare last date created stock
WITH last_stock_date_csql as (
  SELECT MAX(stock_creation_date) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "stock_creation_date" FROM public.stock'
          )
    ),
    last_stock_date_big_query as (
        SELECT MAX(stock_creation_date) FROM `passculture-data-prod.analytics_prod.applicative_database_stock`
    )
SELECT (
        SELECT * FROM last_stock_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_stock_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_stock_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_stock_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare last date created venue
WITH last_venue_date_csql as (
  SELECT MAX(venue_creation_date) FROM EXTERNAL_QUERY(
              'passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection',
              'SELECT "venue_creation_date" FROM public.venue'
          )
    ),
    last_venue_date_big_query as (
        SELECT MAX(venue_creation_date) FROM `passculture-data-prod.analytics_prod.applicative_database_venue`
    )
SELECT (
        SELECT * FROM last_venue_date_csql
    ) as csql_date,
    (
        SELECT * FROM last_venue_date_big_query
    ) as bq_date,
    (
        SELECT TIMESTAMP_DIFF(CAST((SELECT * FROM last_venue_date_csql) as TIMESTAMP), CAST((SELECT * FROM last_venue_date_big_query) as TIMESTAMP), SECOND)
    ) as timestamp_diff

-- compare booking null values in csql and bigquery
SELECT
(
  SELECT "booking_id"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_id" IS NULL')
) as count_null_in_booking_id_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_id IS NULL
) as count_null_in_booking_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_creation_date"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_creation_date" IS NULL')
) as count_null_in_booking_creation_date_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_creation_date IS NULL
) as count_null_in_booking_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_id"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "stock_id" IS NULL')
) as count_null_in_stock_id_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE stock_id IS NULL
) as count_null_in_stock_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_quantity"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_quantity" IS NULL')
) as count_null_in_booking_quantity_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_quantity IS NULL
) as count_null_in_booking_quantity_big_query
UNION ALL
SELECT
(
  SELECT "user_id"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "user_id" IS NULL')
) as count_null_in_user_id_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE user_id IS NULL
) as count_null_in_user_id_big_query
UNION ALL
SELECT
(
  SELECT "booking_amount"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_amount" IS NULL')
) as count_null_in_booking_amount_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_amount IS NULL
) as count_null_in_booking_amount_big_query
UNION ALL
SELECT
(
  SELECT "booking_is_cancelled"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_is_cancelled" IS NULL')
) as count_null_in_booking_is_cancelled_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_is_cancelled IS NULL
) as count_null_in_booking_is_cancelled_big_query
UNION ALL
SELECT
(
  SELECT "booking_is_used"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_is_used" IS NULL')
) as count_null_in_booking_is_used_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_is_used IS NULL
) as count_null_in_booking_is_used_big_query
UNION ALL
SELECT
(
  SELECT "booking_used_date"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_used_date" IS NULL')
) as count_null_in_booking_used_date_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_used_date IS NULL
) as count_null_in_booking_used_date_big_query
UNION ALL
SELECT
(
  SELECT "booking_cancellation_date"
) as column_name,
(
  SELECT count(booking_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT booking_id FROM public.booking WHERE "booking_cancellation_date" IS NULL')
) as count_null_in_booking_cancellation_date_csql,
(
  SELECT count(booking_id) FROM `passculture-data-prod.analytics_prod.applicative_database_booking` WHERE booking_cancellation_date IS NULL
) as count_null_in_booking_cancellation_date_big_query


-- compare mediation null values in csql and bigquery
SELECT
(
  SELECT "thumbCount"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "thumbCount" IS NULL')
) as count_null_in_thumbCount_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE thumbCount IS NULL
) as count_null_in_thumbCount_big_query
UNION ALL
SELECT
(
  SELECT "idAtProviders"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE idAtProviders IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE dateModifiedAtLastProvider IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE id IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE dateCreated IS NULL
) as count_null_in_dateCreated_big_query
UNION ALL
SELECT
(
  SELECT "authorId"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "authorId" IS NULL')
) as count_null_in_authorId_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE authorId IS NULL
) as count_null_in_authorId_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE lastProviderId IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "offerId"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "offerId" IS NULL')
) as count_null_in_offerId_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE offerId IS NULL
) as count_null_in_offerId_big_query
UNION ALL
SELECT
(
  SELECT "credit"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "credit" IS NULL')
) as count_null_in_credit_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE credit IS NULL
) as count_null_in_credit_big_query
UNION ALL
SELECT
(
  SELECT "isActive"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT id FROM public.mediation WHERE "isActive" IS NULL')
) as count_null_in_isActive_csql,
(
  SELECT count(id) FROM `passculture-data-prod.analytics_prod.applicative_database_mediation` WHERE isActive IS NULL
) as count_null_in_isActive_big_query


-- compare offer null values in csql and bigquery
SELECT
  (
    SELECT "offer_id_at_providers"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_id_at_providers" IS NULL')
  ) as count_null_in_offer_id_at_providers_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_id_at_providers IS NULL
  ) as count_null_in_offer_id_at_providers_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_modified_at_last_provider_date"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_modified_at_last_provider_date" IS NULL')
  ) as count_null_in_offer_modified_at_last_provider_date_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_modified_at_last_provider_date IS NULL
  ) as count_null_in_offer_modified_at_last_provider_date_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_id"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_id" IS NULL')
  ) as count_null_in_offer_id_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_id IS NULL
  ) as count_null_in_offer_id_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_creation_date"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_creation_date" IS NULL')
  ) as count_null_in_offer_creation_date_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_creation_date IS NULL
  ) as count_null_in_offer_creation_date_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_product_id"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_product_id" IS NULL')
  ) as count_null_in_offer_product_id_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_product_id IS NULL
  ) as count_null_in_offer_product_id_big_query
  UNION ALL
  SELECT
  (
    SELECT "venue_id"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "venue_id" IS NULL')
  ) as count_null_in_venue_id_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE venue_id IS NULL
  ) as count_null_in_venue_id_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_last_provider_id"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_last_provider_id" IS NULL')
  ) as count_null_in_offer_last_provider_id_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_last_provider_id IS NULL
  ) as count_null_in_offer_last_provider_id_big_query
  UNION ALL
  SELECT
  (
    SELECT "booking_email"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "booking_email" IS NULL')
  ) as count_null_in_booking_email_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE booking_email IS NULL
  ) as count_null_in_booking_email_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_is_active"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_is_active" IS NULL')
  ) as count_null_in_offer_is_active_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_is_active IS NULL
  ) as count_null_in_offer_is_active_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_type"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_type" IS NULL')
  ) as count_null_in_offer_type_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_type IS NULL
  ) as count_null_in_offer_type_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_name"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_name" IS NULL')
  ) as count_null_in_offer_name_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_name IS NULL
  ) as count_null_in_offer_name_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_description"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_description" IS NULL')
  ) as count_null_in_offer_description_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_description IS NULL
  ) as count_null_in_offer_description_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_conditions"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_conditions" IS NULL')
  ) as count_null_in_offer_conditions_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_conditions IS NULL
  ) as count_null_in_offer_conditions_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_age_min"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_age_min" IS NULL')
  ) as count_null_in_offer_age_min_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_age_min IS NULL
  ) as count_null_in_offer_age_min_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_age_max"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_age_max" IS NULL')
  ) as count_null_in_offer_age_max_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_age_max IS NULL
  ) as count_null_in_offer_age_max_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_url"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_url" IS NULL')
  ) as count_null_in_offer_url_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_url IS NULL
  ) as count_null_in_offer_url_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_duration_minutes"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_duration_minutes" IS NULL')
  ) as count_null_in_offer_duration_minutes_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_duration_minutes IS NULL
  ) as count_null_in_offer_duration_minutes_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_is_national"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_is_national" IS NULL')
  ) as count_null_in_offer_is_national_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_is_national IS NULL
  ) as count_null_in_offer_is_national_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_extra_data"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_extra_data" IS NULL')
  ) as count_null_in_offer_extra_data_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_extra_data IS NULL
  ) as count_null_in_offer_extra_data_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_is_duo"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_is_duo" IS NULL')
  ) as count_null_in_offer_is_duo_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_is_duo IS NULL
  ) as count_null_in_offer_is_duo_big_query
  UNION ALL
  SELECT
  (
    SELECT "offer_withdrawal_details"
  ) as column_name,
  (
    SELECT count(offer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offer_id FROM public.offer WHERE "offer_withdrawal_details" IS NULL')
  ) as count_null_in_offer_withdrawal_details_csql,
  (
    SELECT count(offer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offer` WHERE offer_withdrawal_details IS NULL
  ) as count_null_in_offer_withdrawal_details_big_query


-- compare offerer null values in csql and bigquery
SELECT
(
  SELECT "offerer_is_active"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_is_active" IS NULL')
) as count_null_in_offerer_is_active_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_is_active IS NULL
) as count_null_in_offerer_is_active_big_query
UNION ALL
SELECT
(
  SELECT "offerer_thumb_count"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_thumb_count" IS NULL')
) as count_null_in_offerer_thumb_count_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_thumb_count IS NULL
) as count_null_in_offerer_thumb_count_big_query
UNION ALL
SELECT
(
  SELECT "offerer_id_at_providers"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_id_at_providers" IS NULL')
) as count_null_in_offerer_id_at_providers_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_id_at_providers IS NULL
) as count_null_in_offerer_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "offerer_modified_at_last_provider_date"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_modified_at_last_provider_date" IS NULL')
) as count_null_in_offerer_modified_at_last_provider_date_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_modified_at_last_provider_date IS NULL
) as count_null_in_offerer_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "offerer_address"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_address" IS NULL')
) as count_null_in_offerer_address_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_address IS NULL
) as count_null_in_offerer_address_big_query
UNION ALL
SELECT
(
  SELECT "offerer_postal_code"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_postal_code" IS NULL')
) as count_null_in_offerer_postal_code_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_postal_code IS NULL
) as count_null_in_offerer_postal_code_big_query
UNION ALL
SELECT
(
  SELECT "offerer_city"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_city" IS NULL')
) as count_null_in_offerer_city_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_city IS NULL
) as count_null_in_offerer_city_big_query
UNION ALL
SELECT
(
  SELECT "offerer_id"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_id" IS NULL')
) as count_null_in_offerer_id_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_id IS NULL
) as count_null_in_offerer_id_big_query
UNION ALL
SELECT
(
  SELECT "offerer_creation_date"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_creation_date" IS NULL')
) as count_null_in_offerer_creation_date_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_creation_date IS NULL
) as count_null_in_offerer_creation_date_big_query
UNION ALL
SELECT
(
  SELECT "offerer_name"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_name" IS NULL')
) as count_null_in_offerer_name_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_name IS NULL
) as count_null_in_offerer_name_big_query
UNION ALL
SELECT
(
  SELECT "offerer_siren"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_siren" IS NULL')
) as count_null_in_offerer_siren_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_siren IS NULL
) as count_null_in_offerer_siren_big_query
UNION ALL
SELECT
(
  SELECT "offerer_last_provider_id"
) as column_name,
(
  SELECT count(offerer_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT offerer_id FROM public.offerer WHERE "offerer_last_provider_id" IS NULL')
) as count_null_in_offerer_last_provider_id_csql,
(
  SELECT count(offerer_id) FROM `passculture-data-prod.analytics_prod.applicative_database_offerer` WHERE offerer_last_provider_id IS NULL
) as count_null_in_offerer_last_provider_id_big_query


-- compare stock null values in csql and bigquery
SELECT
(
  SELECT "stock_id_at_providers"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_id_at_providers" IS NULL')
) as count_null_in_stock_id_at_providers_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_id_at_providers IS NULL
) as count_null_in_stock_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "stock_modified_at_last_provider_date"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_modified_at_last_provider_date" IS NULL')
) as count_null_in_stock_modified_at_last_provider_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_modified_at_last_provider_date IS NULL
) as count_null_in_stock_modified_at_last_provider_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_id"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_id" IS NULL')
) as count_null_in_stock_id_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_id IS NULL
) as count_null_in_stock_id_big_query
UNION ALL
SELECT
(
  SELECT "stock_modified_date"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_modified_date" IS NULL')
) as count_null_in_stock_modified_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_modified_date IS NULL
) as count_null_in_stock_modified_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_price"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_price" IS NULL')
) as count_null_in_stock_price_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_price IS NULL
) as count_null_in_stock_price_big_query
UNION ALL
SELECT
(
  SELECT "stock_quantity"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_quantity" IS NULL')
) as count_null_in_stock_quantity_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_quantity IS NULL
) as count_null_in_stock_quantity_big_query
UNION ALL
SELECT
(
  SELECT "stock_booking_limit_date"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_booking_limit_date" IS NULL')
) as count_null_in_stock_booking_limit_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_booking_limit_date IS NULL
) as count_null_in_stock_booking_limit_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_last_provider_id"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_last_provider_id" IS NULL')
) as count_null_in_stock_last_provider_id_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_last_provider_id IS NULL
) as count_null_in_stock_last_provider_id_big_query
UNION ALL
SELECT
(
  SELECT "offer_id"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "offer_id" IS NULL')
) as count_null_in_offer_id_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE offer_id IS NULL
) as count_null_in_offer_id_big_query
UNION ALL
SELECT
(
  SELECT "stock_is_soft_deleted"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_is_soft_deleted" IS NULL')
) as count_null_in_stock_is_soft_deleted_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_is_soft_deleted IS NULL
) as count_null_in_stock_is_soft_deleted_big_query
UNION ALL
SELECT
(
  SELECT "stock_beginning_date"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_beginning_date" IS NULL')
) as count_null_in_stock_beginning_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_beginning_date IS NULL
) as count_null_in_stock_beginning_date_big_query
UNION ALL
SELECT
(
  SELECT "stock_creation_date"
) as column_name,
(
  SELECT count(stock_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT stock_id FROM public.stock WHERE "stock_creation_date" IS NULL')
) as count_null_in_stock_creation_date_csql,
(
  SELECT count(stock_id) FROM `passculture-data-prod.analytics_prod.applicative_database_stock` WHERE stock_creation_date IS NULL
) as count_null_in_stock_creation_date_big_query


-- compare venue null values in csql and bigquery
SELECT
(
  SELECT "venue_thumb_count"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_thumb_count" IS NULL')
) as count_null_in_venue_thumb_count_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_thumb_count IS NULL
) as count_null_in_venue_thumb_count_big_query
UNION ALL
SELECT
(
  SELECT "venue_id_at_providers"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_id_at_providers" IS NULL')
) as count_null_in_venue_id_at_providers_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_id_at_providers IS NULL
) as count_null_in_venue_id_at_providers_big_query
UNION ALL
SELECT
(
  SELECT "venue_modified_at_last_provider"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_modified_at_last_provider" IS NULL')
) as count_null_in_venue_modified_at_last_provider_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_modified_at_last_provider IS NULL
) as count_null_in_venue_modified_at_last_provider_big_query
UNION ALL
SELECT
(
  SELECT "venue_address"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_address" IS NULL')
) as count_null_in_venue_address_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_address IS NULL
) as count_null_in_venue_address_big_query
UNION ALL
SELECT
(
  SELECT "venue_postal_code"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_postal_code" IS NULL')
) as count_null_in_venue_postal_code_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_postal_code IS NULL
) as count_null_in_venue_postal_code_big_query
UNION ALL
SELECT
(
  SELECT "venue_city"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_city" IS NULL')
) as count_null_in_venue_city_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_city IS NULL
) as count_null_in_venue_city_big_query
UNION ALL
SELECT
(
  SELECT "venue_id"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_id" IS NULL')
) as count_null_in_venue_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_id IS NULL
) as count_null_in_venue_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_name"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_name" IS NULL')
) as count_null_in_venue_name_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_name IS NULL
) as count_null_in_venue_name_big_query
UNION ALL
SELECT
(
  SELECT "venue_siret"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_siret" IS NULL')
) as count_null_in_venue_siret_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_siret IS NULL
) as count_null_in_venue_siret_big_query
UNION ALL
SELECT
(
  SELECT "venue_department_code"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_department_code" IS NULL')
) as count_null_in_venue_department_code_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_department_code IS NULL
) as count_null_in_venue_department_code_big_query
UNION ALL
SELECT
(
  SELECT "venue_latitude"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_latitude" IS NULL')
) as count_null_in_venue_latitude_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_latitude IS NULL
) as count_null_in_venue_latitude_big_query
UNION ALL
SELECT
(
  SELECT "venue_longitude"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_longitude" IS NULL')
) as count_null_in_venue_longitude_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_longitude IS NULL
) as count_null_in_venue_longitude_big_query
UNION ALL
SELECT
(
  SELECT "venue_managing_offerer_id"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_managing_offerer_id" IS NULL')
) as count_null_in_venue_managing_offerer_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_managing_offerer_id IS NULL
) as count_null_in_venue_managing_offerer_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_booking_email"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_booking_email" IS NULL')
) as count_null_in_venue_booking_email_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_booking_email IS NULL
) as count_null_in_venue_booking_email_big_query
UNION ALL
SELECT
(
  SELECT "venue_last_provider_id"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_last_provider_id" IS NULL')
) as count_null_in_venue_last_provider_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_last_provider_id IS NULL
) as count_null_in_venue_last_provider_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_is_virtual"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_is_virtual" IS NULL')
) as count_null_in_venue_is_virtual_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_is_virtual IS NULL
) as count_null_in_venue_is_virtual_big_query
UNION ALL
SELECT
(
  SELECT "venue_comment"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_comment" IS NULL')
) as count_null_in_venue_comment_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_comment IS NULL
) as count_null_in_venue_comment_big_query
UNION ALL
SELECT
(
  SELECT "venue_public_name"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_public_name" IS NULL')
) as count_null_in_venue_public_name_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_public_name IS NULL
) as count_null_in_venue_public_name_big_query
UNION ALL
SELECT
(
  SELECT "venue_type_id"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_type_id" IS NULL')
) as count_null_in_venue_type_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_type_id IS NULL
) as count_null_in_venue_type_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_label_id"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_label_id" IS NULL')
) as count_null_in_venue_label_id_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_label_id IS NULL
) as count_null_in_venue_label_id_big_query
UNION ALL
SELECT
(
  SELECT "venue_creation_date"
) as column_name,
(
  SELECT count(venue_id) FROM EXTERNAL_QUERY('passculture-data-prod.europe-west1.cloudsql-recommendation-prod-bq-connection', 'SELECT venue_id FROM public.venue WHERE "venue_creation_date" IS NULL')
) as count_null_in_venue_creation_date_csql,
(
  SELECT count(venue_id) FROM `passculture-data-prod.analytics_prod.applicative_database_venue` WHERE venue_creation_date IS NULL
) as count_null_in_venue_creation_date_big_query
