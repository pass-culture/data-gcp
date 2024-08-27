SELECT
    "isActive" as is_active
    , CAST("id" AS varchar(255)) as id
    , CAST("venueProviderId" AS varchar(255)) as venue_provider_id
    , CAST("bookingExternalUrl" AS varchar(255)) as booking_external_url
    , CAST("cancelExternalUrl" AS varchar(255)) as cancel_external_url
    , CAST("notificationExternalUrl" AS varchar(255)) as notification_external_url
FROM public.venue_provider_external_urls