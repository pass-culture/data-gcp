with
TEMP as (
    select distinct
        ENTRY_ID,
        TITLE as HOME_NAME,
        TAG_KEY,
        TAG_VALUE
    from
        {{ ref("int_contentful__tag") }} TAGS
        inner join
            {{ ref("int_contentful__entry") }} ENTRIES
            on
                ENTRIES.ID = TAGS.ENTRY_ID
    where
        CONTENT_TYPE = 'homepageNatif'
)

select
    ENTRY_ID,
    HOME_NAME,
    ARRAY_TO_STRING(HOME_AUDIENCE, ', ') as HOME_AUDIENCE,
    ARRAY_TO_STRING(HOME_CYCLE_VIE_UTILISATEUR, ' , ') as USER_LIFECYCLE_HOME,
    ARRAY_TO_STRING(TYPE_HOME, ', ') as HOME_TYPE
from
    TEMP pivot (ARRAY_AGG(
    TAG_VALUE ignore nulls) for TAG_KEY in (
    'home_audience',
    'home_cycle_vie_utilisateur',
    'type_home'
))
