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
            base.wikidata_image_license_url,
            -- origin flag used to break wikidata_id ties: prefer the artist that
            -- already exists in the applicative base over a freshly added one
            0 as is_from_delta
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
            wikidata_image_license_url,
            1 as is_from_delta
        from artist_delta
        where action in ("add", "update")
    ),

    deduplicated_artist as (
        -- Defensive deduplication: the linkage job can transiently attach the same
        -- wikidata_id to two artist_ids. Keep every artist without a wikidata_id,
        -- and a single canonical artist per wikidata_id (preferring the applicative
        -- base). ml_linkage__future_product_artist_link remaps the dropped
        -- artist_ids onto the survivor kept here.
        select *
        from future_artist
        qualify
            wikidata_id is null
            or row_number() over (
                partition by wikidata_id order by is_from_delta asc, artist_id asc
            )
            = 1
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
from deduplicated_artist
