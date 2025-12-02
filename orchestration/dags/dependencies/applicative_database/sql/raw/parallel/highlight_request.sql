SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("highlightId" as varchar(255)) as highlight_id
    , CAST("mediationUuid" as varchar(255)) as mediation_uuid
    , CAST("highlightDatespan" as varchar(255)) as highlight_datespan
    , CAST("availabilityDatespan" as varchar(255)) as availability_datespan
    , "communicationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS communication_date
FROM public.highlight_request
