SELECT
    CAST("id" AS varchar(255)) AS incident_id
    , CAST("kind" AS varchar(255)) AS kind
    , CAST("status" AS varchar(255)) AS status
    , CAST("details" AS varchar(255)) AS details
    , CAST(venueId AS varchar(255)) AS venue_id
FROM public.finance_incident
