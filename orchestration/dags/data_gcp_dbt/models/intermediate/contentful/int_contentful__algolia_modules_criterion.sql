-- TODO check if this query is still useful
with
    child_tags as (
        select
            id as child_module_id,
            is_geolocated,
            hits_per_page,
            around_radius,
            replace(tags, '\"', "") as tag_name
        from
            {{ ref("int_contentful__entry") }},
            unnest(json_extract_array(tags, '$')) as tags
        where tags is not null and tags != 'nan'
    -- contient uniquement des playlists taggées / exclut les playlists automatiques.
    -- contient envirion 1800 playlists
    -- parmi ces playlists, 30 ont plus d'un tag
    ),

    criterion as (
        select
            child_module_id,
            is_geolocated,
            hits_per_page,
            around_radius,
            ct.tag_name,
            adc.criterion_id,
            adc.offer_id,
            adc.offer_name
        from child_tags ct
        left join
            {{ ref("int_applicative__offer_criterion") }} adc
            on adc.tag_name = ct.tag_name
    ),

    module_ids as (
        select e.id as module_id, e.title as module_name, r.child
        from {{ ref("int_contentful__entry") }} e
        left join {{ ref("int_contentful__relationship") }} r on e.id = r.parent
        where e.content_type = "algolia"
    )

select * except (child_module_id, child)
from module_ids mi
inner join criterion c on c.child_module_id = mi.child
