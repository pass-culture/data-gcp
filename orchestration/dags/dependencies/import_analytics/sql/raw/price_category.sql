SELECT
    CAST("id" AS varchar(255)) AS price_category_id,
    CAST("offerId" AS varchar(255)) AS offer_id,
    "price" AS price,
    CAST("priceCategoryLabelId" AS varchar(255)) AS price_category_label_id
FROM public.price_category
