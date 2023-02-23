SELECT
    CAST("id" AS varchar(255)) as price_category_label_id
    , "label" as label
    , CAST("venueId" AS varchar(255)) AS venue_id
FROM public.price_category_label