with

    artist_delta as (
        select
            artist_id,
            artist_name,
            artist_description,
            artist_biography,
            artist_mediation_uuid,
            wikidata_id,
            wikipedia_url,
            wikidata_image_file_url,
            wikidata_image_author,
            wikidata_image_license,
            wikidata_image_license_url,
            action
        from {{ ref("ml_linkage__delta_artist") }}
    ),

    future_artist as (
        select
            base.artist_id,
            base.artist_name,
            base.artist_description,
            base.artist_biography,
            base.artist_mediation_uuid,
            base.wikidata_id,
            base.wikipedia_url,
            base.wikidata_image_file_url,
            base.wikidata_image_author,
            base.wikidata_image_license,
            base.wikidata_image_license_url
        from {{ ref("int_applicative__artist") }} as base
        where
            not exists (
                select 1 as found
                from artist_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.artist_id = base.artist_id
            )
        union all
        select
            artist_id,
            artist_name,
            artist_description,
            artist_biography,
            artist_mediation_uuid,
            wikidata_id,
            wikipedia_url,
            wikidata_image_file_url,
            wikidata_image_author,
            wikidata_image_license,
            wikidata_image_license_url
        from artist_delta
        where action in ("add", "update")
    )

select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    artist_mediation_uuid,
    wikidata_id,
    wikipedia_url,
    wikidata_image_file_url,
    wikidata_image_author,
    wikidata_image_license,
    wikidata_image_license_url
from future_artist
