with
TEMP as (
    select
        ENTRY_ID,
        COALESCE(TITLE, OFFER_TITLE) as BLOC_NAME,
        CONTENT_TYPE,
        TAG_KEY,
        TAG_VALUE
    from
        {{ ref("int_contentful__tag") }} TAGS
        inner join
            {{ ref("int_contentful__entry") }} ENTRIES
            on
                ENTRIES.ID = TAGS.ENTRY_ID
    where
        CONTENT_TYPE != 'homepageNatif'
)

select
    ENTRY_ID,
    BLOC_NAME,
    CONTENT_TYPE,
    ARRAY_TO_STRING(TYPE_PLAYLIST, ' , ') as PLAYLIST_TYPE,
    ARRAY_TO_STRING(CATEGORIE_OFFRE, ' , ') as OFFER_CATEGORY,
    ARRAY_TO_STRING(PLAYLIST_PORTEE, ' , ') as PLAYLIST_REACH,
    ARRAY_TO_STRING(PLAYLIST_RECCURENCE, ' , ') as PLAYLIST_RECURRENCE
from
    TEMP pivot (ARRAY_AGG(
    TAG_VALUE ignore nulls) for TAG_KEY in (
    'type_playlist',
    'categorie_offre',
    'playlist_portee',
    'playlist_reccurence'
))
