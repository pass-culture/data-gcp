{{ config(materialized="view") }}

with
    formatted_similar_artists as (
        select
            artist_id,
            json_object(
                'artist_id_match', artist_id_match, 'rank', rank_combined
            ) as json_data
        from {{ source("ml_preproc", "similar_artist") }}
        order by artist_id, rank_combined
    )

select artist_id, to_json(array_agg(json_data)) as similar_artists_json
from formatted_similar_artists
group by artist_id
