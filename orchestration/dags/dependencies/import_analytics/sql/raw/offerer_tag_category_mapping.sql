SELECT 
    CAST("id" AS varchar(255)) AS offerer_tag_category_mapping_id
    ,CAST("tagId" AS varchar(255)) AS offerer_tag_id
    ,CAST("categoryId" AS varchar(255)) AS offerer_tag_category_id
FROM public.offerer_tag_category_mapping