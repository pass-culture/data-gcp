with
    temp as (
        select
            entry_id,
            coalesce(title, offer_title) as bloc_name,
            content_type,
            tag_key,
            tag_value
        from {{ ref("int_contentful__tag") }} tags
        inner join
            {{ ref("int_contentful__entry") }} entries on entries.id = tags.entry_id
        where content_type != 'homepageNatif'
    )

select
    entry_id,
    bloc_name,
    content_type,
    array_to_string(type_playlist, ' , ') as playlist_type,
    array_to_string(categorie_offre, ' , ') as offer_category,
    array_to_string(playlist_portee, ' , ') as playlist_reach,
    array_to_string(playlist_reccurence, ' , ') as playlist_recurrence
from
    temp pivot (
        array_agg(tag_value ignore nulls) for tag_key in (
            'type_playlist', 'categorie_offre', 'playlist_portee', 'playlist_reccurence'
        )
    )
