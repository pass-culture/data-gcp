SELECT
    CAST("id" AS varchar(255)) AS accessibility_provider_id
    , CAST("venueId" AS varchar(255)) AS venue_id
    , "externalAccessibilityId" AS external_accessibility_id
    , "externalAccessibilityData" AS external_accessibility_data
    , "lastUpdateAtProvider" AS last_update_at_provider
FROM public.accessibility_provider