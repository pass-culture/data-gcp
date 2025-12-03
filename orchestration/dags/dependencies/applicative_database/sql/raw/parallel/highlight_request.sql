SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("highlightId" as varchar(255)) as highlight_id
FROM public.highlight_request
