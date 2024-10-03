with
    temp as (
        select distinct entry_id, title as home_name, tag_key, tag_value
        from {{ ref("int_contentful__tag") }} tags
        inner join
            {{ ref("int_contentful__entry") }} entries on entries.id = tags.entry_id
        where content_type = 'homepageNatif'
    )

select
    entry_id,
    home_name,
    array_to_string(home_audience, ', ') as home_audience,
    array_to_string(home_cycle_vie_utilisateur, ' , ') as user_lifecycle_home,
    array_to_string(type_home, ', ') as home_type
from
    temp pivot (
        array_agg(tag_value ignore nulls) for tag_key
        in ('home_audience', 'home_cycle_vie_utilisateur', 'type_home')
    )
