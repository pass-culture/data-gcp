SELECT
    "isActive" as is_active
    , CAST("id" AS varchar(255)) as id
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST("providerId" AS varchar(255)) as provider_id
    , CAST("venueIdAtOfferProvider" AS varchar(255)) as venue_id_at_offer_provider
    , "lastSyncDate" as last_sync_date
FROM public.venue_provider