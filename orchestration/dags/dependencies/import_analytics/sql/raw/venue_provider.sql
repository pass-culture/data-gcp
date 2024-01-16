SELECT
    "isActive" as is_active
    , CAST("id" AS varchar(255)) as id
    , "dateModifiedAtLastProvider" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as date_modified_at_last_provider
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST("providerId" AS varchar(255)) as provider_id
    , CAST("venueIdAtOfferProvider" AS varchar(255)) as venue_id_at_offer_provider
    , "lastSyncDate" as last_sync_date
    , CAST("lastProviderId" AS varchar(255)) as last_provider_id
    , "fieldsUpdated" as fields_updated
FROM public.venue_provider