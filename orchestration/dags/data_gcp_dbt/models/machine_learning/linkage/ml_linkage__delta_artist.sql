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

    added_delta_artist as (
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
        from casted_delta_artist
        where action = 'add'
    ),

    removed_or_updated_delta_artist as (
        select
            casted_delta_artist.artist_id,
            casted_delta_artist.artist_name,
            casted_delta_artist.artist_description,
            casted_delta_artist.artist_biography,
            casted_delta_artist.artist_mediation_uuid,
            casted_delta_artist.wikidata_id,
            casted_delta_artist.wikipedia_url,
            casted_delta_artist.wikidata_image_file_url,
            casted_delta_artist.wikidata_image_author,
            casted_delta_artist.wikidata_image_license,
            casted_delta_artist.wikidata_image_license_url,
            casted_delta_artist.spotify_id,
            casted_delta_artist.isni_id,
            casted_delta_artist.apple_music_id,
            casted_delta_artist.deezer_id,
            casted_delta_artist.genius_id,
            casted_delta_artist.soundcloud_id,
            casted_delta_artist.action,
            casted_delta_artist.comment
        from casted_delta_artist
        inner join
            {{ source("raw", "applicative_database_artist") }}
            as applicative_database_artist
            on casted_delta_artist.artist_id = applicative_database_artist.artist_id
        where casted_delta_artist.action in ('update', 'remove')
    ),

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
    )

select
    added_delta_artist.artist_id,
    added_delta_artist.artist_name,
    added_delta_artist.artist_description,
    added_delta_artist.artist_biography,
    added_delta_artist.artist_mediation_uuid,
    added_delta_artist.wikidata_id,
    added_delta_artist.wikipedia_url,
    added_delta_artist.wikidata_image_file_url,
    added_delta_artist.wikidata_image_author,
    added_delta_artist.wikidata_image_license,
    added_delta_artist.wikidata_image_license_url,
    added_delta_artist.spotify_id,
    added_delta_artist.isni_id,
    added_delta_artist.apple_music_id,
    added_delta_artist.deezer_id,
    added_delta_artist.genius_id,
    added_delta_artist.soundcloud_id,
    added_delta_artist.action,
    added_delta_artist.comment
from added_delta_artist
union all
select
    removed_or_updated_delta_artist.artist_id,
    removed_or_updated_delta_artist.artist_name,
    removed_or_updated_delta_artist.artist_description,
    removed_or_updated_delta_artist.artist_biography,
    removed_or_updated_delta_artist.artist_mediation_uuid,
    removed_or_updated_delta_artist.wikidata_id,
    removed_or_updated_delta_artist.wikipedia_url,
    removed_or_updated_delta_artist.wikidata_image_file_url,
    removed_or_updated_delta_artist.wikidata_image_author,
    removed_or_updated_delta_artist.wikidata_image_license,
    removed_or_updated_delta_artist.wikidata_image_license_url,
    removed_or_updated_delta_artist.spotify_id,
    removed_or_updated_delta_artist.isni_id,
    removed_or_updated_delta_artist.apple_music_id,
    removed_or_updated_delta_artist.deezer_id,
    removed_or_updated_delta_artist.genius_id,
    removed_or_updated_delta_artist.soundcloud_id,
    removed_or_updated_delta_artist.action,
    removed_or_updated_delta_artist.comment
from removed_or_updated_delta_artist
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
    artists_to_remove.spotify_id,
    artists_to_remove.isni_id,
    artists_to_remove.apple_music_id,
    artists_to_remove.deezer_id,
    artists_to_remove.genius_id,
    artists_to_remove.soundcloud_id,
    artists_to_remove.action,
    artists_to_remove.comment
from artists_to_remove
