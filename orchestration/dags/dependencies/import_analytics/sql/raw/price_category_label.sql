SELECT
    CAST("id" AS varchar(255)) AS price_category_label_id,
    "label" AS label,
    CAST("venueId" AS varchar(255)) AS venue_id
FROM public.price_category_label
