SELECT
    CAST("id" AS varchar(255)) as price_category_id
    , CAST("offerId" AS varchar(255)) as offer_id
    , "price" as price
    , CAST("priceCategoryLabelId" AS varchar(255)) AS price_category_label_id
FROM public.price_category