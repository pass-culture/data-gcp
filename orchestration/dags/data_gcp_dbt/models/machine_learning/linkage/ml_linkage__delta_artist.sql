{{ config(**custom_table_config(materialized="view")) }}

with
    artist_linked as (
        select distinct artist_id, true as is_linked
        from {{ ref("int_applicative__product_artist_link") }}
    ),

    artists_to_remove as (
        select
            artist.artist_id,
            artist.artist_name,
            artist.artist_description,
            artist.artist_biography,
            artist.artist_mediation_uuid,
            artist.wikidata_id,
            artist.wikipedia_url,
            artist.wikidata_image_file_url,
            artist.wikidata_image_license,
            artist.wikidata_image_license_url,
            artist.wikidata_image_author,
            'remove' as action,  -- noqa: RF04
            'artist not linked to any product' as comment  -- noqa: RF04
        from {{ ref("int_applicative__artist") }} as artist
        left join artist_linked using (artist_id)
        where artist_linked.is_linked is null
        order by artist.artist_name
    )

select
    cast(artist_id as string) as artist_id,
    cast(artist_name as string) as artist_name,
    cast(artist_description as string) as artist_description,
    cast(artist_biography as string) as artist_biography,
    cast(artist_mediation_uuid as string) as artist_mediation_uuid,
    cast(wikidata_id as string) as wikidata_id,
    cast(wikipedia_url as string) as wikipedia_url,
    cast(wikidata_image_file_url as string) as wikidata_image_file_url,
    cast(wikidata_image_author as string) as wikidata_image_author,
    cast(wikidata_image_license as string) as wikidata_image_license,
    cast(wikidata_image_license_url as string) as wikidata_image_license_url,
    cast(action as string) as action,  -- noqa: RF04
    cast(comment as string) as comment  -- noqa: RF04
from {{ source("ml_preproc", "delta_artist") }}
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
    action,
    comment
from artists_to_remove
