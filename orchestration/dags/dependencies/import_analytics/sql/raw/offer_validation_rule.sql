SELECT 
    cast("id" as VARCHAR(255)) as id,
    "name" as name,
    "dateModified" at time zone \'UTC\' at time zone \'Europe/Paris\' as date_modified,
    "latestAuthorId" as latest_author_id
FROM public.offer_validation_rule