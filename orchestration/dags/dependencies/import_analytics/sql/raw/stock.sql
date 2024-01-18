SELECT
    CAST("idAtProviders" AS varchar(255)) AS stock_id_at_providers 
    , "dateModifiedAtLastProvider" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS stock_modified_at_last_provider_date
    , CAST("id" AS varchar(255)) AS stock_id
    , "dateModified" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS stock_modified_date
    , "price" AS stock_price
    , "quantity" AS stock_quantity
    , "bookingLimitDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS stock_booking_limit_date
    , CAST("lastProviderId" AS varchar(255)) AS stock_last_provider_id
    , CAST("offerId" AS varchar(255)) AS offer_id
    , "isSoftDeleted" AS stock_is_soft_deleted
    , "beginningDatetime" AS stock_beginning_date
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS stock_creation_date
    , "fieldsUpdated" AS stock_fields_updated
    , CAST("priceCategoryId" AS varchar(255)) AS price_category_id
    , BTRIM(array_to_string("features", \',\'), \'{\') AS stock_features
FROM public.stock