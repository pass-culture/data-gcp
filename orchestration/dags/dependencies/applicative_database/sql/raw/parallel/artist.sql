SELECT
    "id" as artist_id
    ,"name" as artist_name
    ,"description" as artist_description
    ,"image" as wikidata_image_file_url
    ,"image_license" as woikidata_image_license
    ,"image_license_url" as wikidata_image_license_url
    ,"image_author" as wikidata_image_author
    ,"date_created" as date_created
    ,"date_modified" as date_modified
FROM public.artist
