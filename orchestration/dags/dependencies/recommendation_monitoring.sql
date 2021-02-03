-- compare booking volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              ' SELECT CAST("id" AS varchar(255)) FROM public.booking'
          )
  ) as count_booking_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking`
  ) as count_booking_from_big_query

-- compare mediation volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              ' SELECT CAST("id" AS varchar(255)) FROM public.mediation'
          )
  ) as count_mediation_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation`
  ) as count_mediation_from_big_query

-- compare offer volumetry
SELECT (
    SELECT count(*) FROM EXTERNAL_QUERY(
                'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
                ' SELECT CAST("id" AS varchar(255)) FROM public.offer'
            )
  ) as count_offer_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer`
  ) as count_offer_from_big_query

-- compare offerer volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              ' SELECT CAST("id" AS varchar(255)) FROM public.offerer'
          )
  ) as count_offerer_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer`
  ) as count_offerer_from_big_query

-- compare stock volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              ' SELECT CAST("id" AS varchar(255)) FROM public.stock'
          )
  ) as count_stock_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock`
  ) as count_stock_from_big_query

-- compare venue volumetry
SELECT (
  SELECT count(*) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              ' SELECT CAST("id" AS varchar(255)) FROM public.venue'
          )
  ) as count_venue_from_csql,
  (
    SELECT count(*) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue`
  ) as count_venue_from_big_query

-- compare past_recommended_offers volumetry
SELECT(
  SELECT count(*) as count_past_recommended_offers FROM EXTERNAL_QUERY(
                  'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
                  ' SELECT CAST("id" AS varchar(255)) FROM public.past_recommended_offers'
              )
  ) as count_past_recommended_offers_csql,
  (
    SELECT count(*) as count_past_recommended_offers FROM `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`
  ) as count_past_recommended_offers_csql_bigquery

-- compare last date created booking
WITH last_booking_date_csql as (
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.booking'
          )
    ),
    last_booking_date_big_query as (
        SELECT MAX(booking_creation_date) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking`
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
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.mediation'
          )
    ),
    last_mediation_date_big_query as (
        SELECT MAX(dateCreated) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation`
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
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.offer'
          )
    ),
    last_offer_date_big_query as (
        SELECT MAX(offer_creation_date) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer`
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
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.offerer'
          )
    ),
    last_offerer_date_big_query as (
        SELECT MAX(offerer_creation_date) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer`
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
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.stock'
          )
    ),
    last_stock_date_big_query as (
        SELECT MAX(stock_creation_date) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock`
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
  SELECT MAX(dateCreated) FROM EXTERNAL_QUERY(
              'europe-west1.cloud_SQL_pcdata-poc-csql-recommendation',
              'SELECT "dateCreated" FROM public.venue'
          )
    ),
    last_venue_date_big_query as (
        SELECT MAX(venue_creation_date) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue`
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
  SELECT "isActive"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "isActive" IS NULL')
) as count_null_in_isActive_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "isActive" IS NULL
) as count_null_in_isActive_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "dateCreated" IS NULL
) as count_null_in_dateCreated_big_query
UNION ALL
SELECT
(
  SELECT "recommendationId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "recommendationId" IS NULL')
) as count_null_in_recommendationId_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "recommendationId" IS NULL
) as count_null_in_recommendationId_big_query
UNION ALL
SELECT
(
  SELECT "stockId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "stockId" IS NULL')
) as count_null_in_stockId_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "stockId" IS NULL
) as count_null_in_stockId_big_query
UNION ALL
SELECT
(
  SELECT "quantity"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "quantity" IS NULL')
) as count_null_in_quantity_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "quantity" IS NULL
) as count_null_in_quantity_big_query
UNION ALL
SELECT
(
  SELECT "token"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "token" IS NULL')
) as count_null_in_token_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "token" IS NULL
) as count_null_in_token_big_query
UNION ALL
SELECT
(
  SELECT "userId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "userId" IS NULL')
) as count_null_in_userId_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "userId" IS NULL
) as count_null_in_userId_big_query
UNION ALL
SELECT
(
  SELECT "amount"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "amount" IS NULL')
) as count_null_in_amount_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "amount" IS NULL
) as count_null_in_amount_big_query
UNION ALL
SELECT
(
  SELECT "isCancelled"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "isCancelled" IS NULL')
) as count_null_in_isCancelled_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "isCancelled" IS NULL
) as count_null_in_isCancelled_big_query
UNION ALL
SELECT
(
  SELECT "isUsed"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "isUsed" IS NULL')
) as count_null_in_isUsed_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "isUsed" IS NULL
) as count_null_in_isUsed_big_query
UNION ALL
SELECT
(
  SELECT "dateUsed"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.booking WHERE "dateUsed" IS NULL')
) as count_null_in_dateUsed_csql,
(
  SELECT count(booking_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_booking` WHERE "dateUsed" IS NULL
) as count_null_in_dateUsed_big_query


-- compare mediation null values in csql and bigquery
SELECT
(
  SELECT "thumbCount"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "thumbCount" IS NULL')
) as count_null_in_thumbCount_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "thumbCount" IS NULL
) as count_null_in_thumbCount_big_query
UNION ALL
SELECT
(
  SELECT "idAtProviders"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "idAtProviders" IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "dateModifiedAtLastProvider" IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "dateCreated" IS NULL
) as count_null_in_dateCreated_big_query
UNION ALL
SELECT
(
  SELECT "authorId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "authorId" IS NULL')
) as count_null_in_authorId_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "authorId" IS NULL
) as count_null_in_authorId_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "lastProviderId" IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "offerId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "offerId" IS NULL')
) as count_null_in_offerId_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "offerId" IS NULL
) as count_null_in_offerId_big_query
UNION ALL
SELECT
(
  SELECT "credit"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "credit" IS NULL')
) as count_null_in_credit_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "credit" IS NULL
) as count_null_in_credit_big_query
UNION ALL
SELECT
(
  SELECT "isActive"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.mediation WHERE "isActive" IS NULL')
) as count_null_in_isActive_csql,
(
  SELECT count(id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_mediation` WHERE "isActive" IS NULL
) as count_null_in_isActive_big_query


-- compare offer null values in csql and bigquery
SELECT
(
  SELECT "idAtProviders"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "idAtProviders" IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "dateModifiedAtLastProvider" IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "dateCreated" IS NULL
) as count_null_in_dateCreated_big_query
UNION ALL
SELECT
(
  SELECT "productId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "productId" IS NULL')
) as count_null_in_productId_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "productId" IS NULL
) as count_null_in_productId_big_query
UNION ALL
SELECT
(
  SELECT "venueId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "venueId" IS NULL')
) as count_null_in_venueId_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "venueId" IS NULL
) as count_null_in_venueId_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "lastProviderId" IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "bookingEmail"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "bookingEmail" IS NULL')
) as count_null_in_bookingEmail_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "bookingEmail" IS NULL
) as count_null_in_bookingEmail_big_query
UNION ALL
SELECT
(
  SELECT "isActive"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "isActive" IS NULL')
) as count_null_in_isActive_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "isActive" IS NULL
) as count_null_in_isActive_big_query
UNION ALL
SELECT
(
  SELECT "type"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "type" IS NULL')
) as count_null_in_type_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "type" IS NULL
) as count_null_in_type_big_query
UNION ALL
SELECT
(
  SELECT "name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "name" IS NULL')
) as count_null_in_name_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "name" IS NULL
) as count_null_in_name_big_query
UNION ALL
SELECT
(
  SELECT "description"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offer WHERE "description" IS NULL')
) as count_null_in_description_csql,
(
  SELECT count(offer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offer` WHERE "description" IS NULL
) as count_null_in_description_big_query

-- compare offerer null values in csql and bigquery
SELECT
(
  SELECT "thumbCount"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "thumbCount" IS NULL')
) as count_null_in_thumbCount_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "thumbCount" IS NULL
) as count_null_in_thumbCount_big_query
UNION ALL
SELECT
(
  SELECT "firstThumbDominantColor"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "firstThumbDominantColor" IS NULL')
) as count_null_in_firstThumbDominantColor_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "firstThumbDominantColor" IS NULL
) as count_null_in_firstThumbDominantColor_big_query
UNION ALL
SELECT
(
  SELECT "idAtProviders"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "idAtProviders" IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "dateModifiedAtLastProvider" IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "name" IS NULL')
) as count_null_in_name_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "name" IS NULL
) as count_null_in_name_big_query
UNION ALL
SELECT
(
  SELECT "address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "address" IS NULL')
) as count_null_in_address_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "address" IS NULL
) as count_null_in_address_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "lastProviderId" IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "postalCode"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "postalCode" IS NULL')
) as count_null_in_postalCode_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "postalCode" IS NULL
) as count_null_in_postalCode_big_query
UNION ALL
SELECT
(
  SELECT "city"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "city" IS NULL')
) as count_null_in_city_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "city" IS NULL
) as count_null_in_city_big_query
UNION ALL
SELECT
(
  SELECT "siren"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "siren" IS NULL')
) as count_null_in_siren_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "siren" IS NULL
) as count_null_in_siren_big_query
UNION ALL
SELECT
(
  SELECT "isActive"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.offerer WHERE "isActive" IS NULL')
) as count_null_in_isActive_csql,
(
  SELECT count(offerer_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_offerer` WHERE "isActive" IS NULL
) as count_null_in_isActive_big_query


-- compare stock null values in csql and bigquery
SELECT
(
  SELECT "idAtProviders"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "idAtProviders" IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "dateModifiedAtLastProvider" IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "dateModified"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "dateModified" IS NULL')
) as count_null_in_dateModified_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "dateModified" IS NULL
) as count_null_in_dateModified_big_query
UNION ALL
SELECT
(
  SELECT "price"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "price" IS NULL')
) as count_null_in_price_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "price" IS NULL
) as count_null_in_price_big_query
UNION ALL
SELECT
(
  SELECT "quantity"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "quantity" IS NULL')
) as count_null_in_quantity_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "quantity" IS NULL
) as count_null_in_quantity_big_query
UNION ALL
SELECT
(
  SELECT "bookingLimitDatetime"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "bookingLimitDatetime" IS NULL')
) as count_null_in_bookingLimitDatetime_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "bookingLimitDatetime" IS NULL
) as count_null_in_bookingLimitDatetime_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "lastProviderId" IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "offerId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "offerId" IS NULL')
) as count_null_in_offerId_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "offerId" IS NULL
) as count_null_in_offerId_big_query
UNION ALL
SELECT
(
  SELECT "isSoftDeleted"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "isSoftDeleted" IS NULL')
) as count_null_in_isSoftDeleted_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "isSoftDeleted" IS NULL
) as count_null_in_isSoftDeleted_big_query
UNION ALL
SELECT
(
  SELECT "beginningDatetime"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "beginningDatetime" IS NULL')
) as count_null_in_beginningDatetime_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "beginningDatetime" IS NULL
) as count_null_in_beginningDatetime_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.stock WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(stock_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_stock` WHERE "dateCreated" IS NULL
) as count_null_in_dateCreated_big_query


-- compare venue null values in csql and bigquery
SELECT
(
  SELECT "thumbCount"
) as column_name,
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "thumbCount" IS NULL')
) as count_null_in_thumbCount_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "thumbCount" IS NULL
) as count_null_in_thumbCount_big_query
UNION ALL
SELECT
(
  SELECT "firstThumbDominantColor"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "firstThumbDominantColor" IS NULL')
) as count_null_in_firstThumbDominantColor_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "firstThumbDominantColor" IS NULL
) as count_null_in_firstThumbDominantColor_big_query
UNION ALL
SELECT
(
  SELECT "idAtProviders"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "idAtProviders" IS NULL')
) as count_null_in_idAtProviders_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "idAtProviders" IS NULL
) as count_null_in_idAtProviders_big_query
UNION ALL
SELECT
(
  SELECT "dateModifiedAtLastProvider"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "dateModifiedAtLastProvider" IS NULL')
) as count_null_in_dateModifiedAtLastProvider_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "dateModifiedAtLastProvider" IS NULL
) as count_null_in_dateModifiedAtLastProvider_big_query
UNION ALL
SELECT
(
  SELECT "id"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "id" IS NULL')
) as count_null_in_id_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "id" IS NULL
) as count_null_in_id_big_query
UNION ALL
SELECT
(
  SELECT "name"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "name" IS NULL')
) as count_null_in_name_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "name" IS NULL
) as count_null_in_name_big_query
UNION ALL
SELECT
(
  SELECT "address"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "address" IS NULL')
) as count_null_in_address_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "address" IS NULL
) as count_null_in_address_big_query
UNION ALL
SELECT
(
  SELECT "latitude"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "latitude" IS NULL')
) as count_null_in_latitude_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "latitude" IS NULL
) as count_null_in_latitude_big_query
UNION ALL
SELECT
(
  SELECT "longitude"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "longitude" IS NULL')
) as count_null_in_longitude_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "longitude" IS NULL
) as count_null_in_longitude_big_query
UNION ALL
SELECT
(
  SELECT "lastProviderId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "lastProviderId" IS NULL')
) as count_null_in_lastProviderId_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "lastProviderId" IS NULL
) as count_null_in_lastProviderId_big_query
UNION ALL
SELECT
(
  SELECT "departementCode"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "departementCode" IS NULL')
) as count_null_in_departementCode_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "departementCode" IS NULL
) as count_null_in_departementCode_big_query
UNION ALL
SELECT
(
  SELECT "postalCode"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "postalCode" IS NULL')
) as count_null_in_postalCode_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "postalCode" IS NULL
) as count_null_in_postalCode_big_query
UNION ALL
SELECT
(
  SELECT "city"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "city" IS NULL')
) as count_null_in_city_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "city" IS NULL
) as count_null_in_city_big_query
UNION ALL
SELECT
(
  SELECT "siret"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "siret" IS NULL')
) as count_null_in_siret_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "siret" IS NULL
) as count_null_in_siret_big_query
UNION ALL
SELECT
(
  SELECT "managingOffererId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "managingOffererId" IS NULL')
) as count_null_in_managingOffererId_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "managingOffererId" IS NULL
) as count_null_in_managingOffererId_big_query
UNION ALL
SELECT
(
  SELECT "bookingEmail"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "bookingEmail" IS NULL')
) as count_null_in_bookingEmail_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "bookingEmail" IS NULL
) as count_null_in_bookingEmail_big_query
UNION ALL
SELECT
(
  SELECT "isVirtual"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "isVirtual" IS NULL')
) as count_null_in_isVirtual_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "isVirtual" IS NULL
) as count_null_in_isVirtual_big_query
UNION ALL
SELECT
(
  SELECT "comment"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "comment" IS NULL')
) as count_null_in_comment_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "comment" IS NULL
) as count_null_in_comment_big_query
UNION ALL
SELECT
(
  SELECT "validationToken"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "validationToken" IS NULL')
) as count_null_in_validationToken_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "validationToken" IS NULL
) as count_null_in_validationToken_big_query
UNION ALL
SELECT
(
  SELECT "publicName"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "publicName" IS NULL')
) as count_null_in_publicName_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "publicName" IS NULL
) as count_null_in_publicName_big_query
UNION ALL
SELECT
(
  SELECT "venueTypeId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "venueTypeId" IS NULL')
) as count_null_in_venueTypeId_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "venueTypeId" IS NULL
) as count_null_in_venueTypeId_big_query
UNION ALL
SELECT
(
  SELECT "venueLabelId"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "venueLabelId" IS NULL')
) as count_null_in_venueLabelId_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "venueLabelId" IS NULL
) as count_null_in_venueLabelId_big_query
UNION ALL
SELECT
(
  SELECT "dateCreated"
),
(
  SELECT count(id) FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_pcdata-poc-csql-recommendation', 'SELECT id FROM public.venue WHERE "dateCreated" IS NULL')
) as count_null_in_dateCreated_csql,
(
  SELECT count(venue_id) FROM `pass-culture-app-projet-test.analytics_sbx.applicative_database_venue` WHERE "dateCreated" IS NULL
) as count_null_in_dateCreated_big_query
