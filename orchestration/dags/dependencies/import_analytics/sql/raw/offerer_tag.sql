SELECT
    CAST("id" AS varchar(255)) AS offerer_tag_id,
    "name" AS offerer_tag_name,
    "label" AS offerer_tag_label,
    "description" AS offerer_tag_description
FROM public.offerer_tag
