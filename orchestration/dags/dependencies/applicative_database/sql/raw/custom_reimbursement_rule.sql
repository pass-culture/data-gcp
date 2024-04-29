SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("offerId" AS varchar(255)) as offer_id
    , "amount" AS amount
    , LOWER("timespan") AS start_time
    , UPPER("timespan") AS end_time
    , "rate" AS rate
    , CAST("offererId" AS varchar(255)) as offerer_id
    , "subcategories" AS subcategories
    , CAST("venueId" AS varchar(255)) as venue_id
FROM public.custom_reimbursement_rule