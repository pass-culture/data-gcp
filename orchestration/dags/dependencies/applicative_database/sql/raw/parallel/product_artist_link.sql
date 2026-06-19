SELECT
    "id" as product_artist_link_id
    ,"product_id" as offer_product_id
    ,"artist_id"
    ,"artist_type"
    ,"date_created" AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris' as date_created,
    ,"date_modified" AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris' as date_modified,
FROM public.artist_product_link
