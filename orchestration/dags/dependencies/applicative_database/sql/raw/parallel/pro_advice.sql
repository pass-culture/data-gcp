SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("offerId" AS varchar(255)) as offer_id
    , CAST("venueId" AS varchar(255)) as venue_id
    , CAST("content" AS varchar(255)) as advice_content
    , CAST("author" as varchar(255)) as advice_author
    , "updatedAt" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as updated_at
FROM public.pro_advice
