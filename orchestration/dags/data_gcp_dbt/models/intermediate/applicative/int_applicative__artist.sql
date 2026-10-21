select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    artist_mediation_uuid,
    artist_computed_image,
    wikidata_id,
    wikipedia_url,
    wikidata_image_file_url,
    wikidata_image_license,
    wikidata_image_license_url,
    wikidata_image_author,
    artist_app_search_score,
    artist_pro_search_score,
    date(date_created) as creation_date,
    date(date_modified) as modification_date,
    array_to_string(
        array(
            select part
            from
                unnest(
                    split(
                        regexp_replace(
                            lower(
                                -- 1. Clean accents
                                regexp_replace(
                                    normalize(artist_name, nfd), r'\p{M}', ''
                                )
                            -- 2. Replace spaces or hyphens by a unique
                            -- separator
                            ),
                            r'[\s-]+',
                            ' '
                        ),
                        ' '
                    )
                ) as part
            -- 3. Filter to avoid empty strings if the name starts/ends with a
            -- hyphen
            where part != ''
            order by part
        ),
        '|'
    ) as normalized_artist_name
from {{ source("raw", "applicative_database_artist") }}
