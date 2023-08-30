SELECT
    CAST("id" AS varchar(255)) AS id
    , CAST("title" AS varchar(255)) AS title
    , CAST("ean" AS varchar(255)) AS ean
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as product_date_created
    , CAST("comment" AS varchar(255)) AS comment
    , CAST("authorId" AS varchar(255)) AS author_id
FROM public.product_whitelist