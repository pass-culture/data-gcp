with
    casted_delta_artist as (
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
            cast(spotify_id as string) as spotify_id,
            cast(isni_id as string) as isni_id,
            cast(apple_music_id as string) as apple_music_id,
            cast(deezer_id as string) as deezer_id,
            cast(genius_id as string) as genius_id,
            cast(soundcloud_id as string) as soundcloud_id,
            cast(action as string) as action,  -- noqa: disable=RF04
            cast(comment as string) as comment
        from {{ source("ml_preproc", "delta_artist") }}
    ),

    artist_linked as (
        select distinct artist_id, true as is_linked
        from {{ ref("int_applicative__product_artist_link") }}
    ),

    -- remove artist with no product link and no wikidata id
    orphan_artist_to_remove as (
        select
            artist.artist_id,
            artist.artist_name,
            artist.artist_description,
            artist.artist_biography,
            artist.artist_mediation_uuid,
            artist.wikidata_id,
            artist.wikipedia_url,
            artist.wikidata_image_file_url,
            artist.wikidata_image_author,
            artist.wikidata_image_license,
            artist.wikidata_image_license_url,
            cast(null as string) as spotify_id,
            cast(null as string) as isni_id,
            cast(null as string) as apple_music_id,
            cast(null as string) as deezer_id,
            cast(null as string) as genius_id,
            cast(null as string) as soundcloud_id,
            'remove' as action,
            'artist not linked to any product' as comment
        from {{ ref("int_applicative__artist") }} as artist
        left join artist_linked using (artist_id)
        where
            artist_linked.is_linked is null
            and artist.wikidata_id is null
            -- an artist already carried by the delta must not be emitted twice
            and not exists (
                select 1 as found
                from casted_delta_artist as delta
                where delta.artist_id = artist.artist_id
            )
    ),

    delta_artist as (
        select *
        from casted_delta_artist
        union all
        select *
        from orphan_artist_to_remove
    ),

    added_delta_artist as (select * from delta_artist where action = 'add'),

    removed_or_updated_delta_artist as (
        select
            delta_artist.* replace (
                -- on a remove the delta may not carry the name, take it from
                -- applicative
                case
                    when delta_artist.action = 'remove'
                    then artist.artist_name
                    else delta_artist.artist_name
                end as artist_name
            )
        from delta_artist
        inner join
            {{ ref("int_applicative__artist") }} as artist
            on delta_artist.artist_id = artist.artist_id
        where delta_artist.action in ('update', 'remove')
    ),

    final_delta_artist as (
        select *
        from added_delta_artist
        union all
        select *
        from removed_or_updated_delta_artist
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
    wikidata_image_license_url,
    spotify_id,
    isni_id,
    apple_music_id,
    deezer_id,
    genius_id,
    soundcloud_id,
    action,
    comment
from final_delta_artist
