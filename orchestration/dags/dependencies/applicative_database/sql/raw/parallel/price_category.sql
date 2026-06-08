SELECT
    CAST("id" AS varchar(255)) as price_category_id
    , CAST("offerId" AS varchar(255)) as offer_id
    , "price" as price
    , "label" as price_category_label
FROM public.price_category
