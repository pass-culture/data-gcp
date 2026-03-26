SELECT
    cast("id" AS varchar(255)) as offer_quality_id
    ,cast("offerId" as VARCHAR(255)) as offer_id
    ,"completionScore" as completion_score
    ,"updatedAt" as updated_at
FROM public.offer_quality
