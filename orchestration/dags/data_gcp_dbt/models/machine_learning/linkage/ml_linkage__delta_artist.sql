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
    delta_artist.artist_id,
    delta_artist.artist_name,
    delta_artist.artist_description,
    delta_artist.artist_biography,
    delta_artist.artist_mediation_uuid,
    delta_artist.wikidata_id,
    delta_artist.wikipedia_url,
    delta_artist.wikidata_image_file_url,
    delta_artist.wikidata_image_author,
    delta_artist.wikidata_image_license,
    delta_artist.wikidata_image_license_url,
    delta_artist.action,
    delta_artist.comment
from {{ source("ml_preproc", "delta_artist") }} as delta_artist
union all
select
    artists_to_remove.artist_id,
    artists_to_remove.artist_name,
    artists_to_remove.artist_description,
    artists_to_remove.artist_biography,
    artists_to_remove.artist_mediation_uuid,
    artists_to_remove.wikidata_id,
    artists_to_remove.wikipedia_url,
    artists_to_remove.wikidata_image_file_url,
    artists_to_remove.wikidata_image_author,
    artists_to_remove.wikidata_image_license,
    artists_to_remove.wikidata_image_license_url,
    artists_to_remove.action,
    artists_to_remove.comment
from artists_to_remove
