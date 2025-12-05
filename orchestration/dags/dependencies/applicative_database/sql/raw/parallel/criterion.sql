SELECT
    CAST("id" AS varchar(255)) as id
    , "name"
    , "description"
    , "endDateTime"
    , "startDateTime"
    , cast("highlightId" AS varchar(255)) as highlight_id
FROM public.criterion
