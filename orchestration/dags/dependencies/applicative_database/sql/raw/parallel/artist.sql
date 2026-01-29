SELECT
    "id" as artist_id
    ,"name" as artist_name
    ,"description" as artist_description
    ,"biography" as artist_biography
    ,"mediation_uuid" as artist_mediation_uuid
    ,"wikidata_id"
    ,"wikipedia_url"
    ,"image" as wikidata_image_file_url
    ,"image_license" as wikidata_image_license
    ,"image_license_url" as wikidata_image_license_url
    ,"image_author" as wikidata_image_author
    ,"app_search_score" as artist_app_search_score
    ,"pro_search_score" as artist_pro_search_score
    ,"date_created"
    ,"date_modified"
FROM public.artist
