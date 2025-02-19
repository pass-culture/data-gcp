with
    collection_status as (
        select collection_id, collection_name
        from {{ source("raw", "metabase_collection") }}
        where
            -- the collection is not located in Temporary Archive directory.
            concat(location, collection_id) not like '/610%'
            -- the collection is not located in Metabase's native archive.
            and archived = false
            -- the collections are public (i.e., remove personal collections)
            and personal_owner_id is null
    )

select
    cs.collection_id as metabase_collection_id,
    cs.collection_name as metabase_collection_name,
    count(distinct dq.card_id) as total_metabase_cards,
    count(distinct dq.metabase_user_id) as total_metabase_users,
    count(*) as total_metabase_queries
from {{ ref("int_metabase__daily_query") }} as dq
inner join collection_status as cs on dq.card_collection_id = cs.collection_id
where date(dq.execution_date) >= date_sub(current_date, interval 90 day)
group by 1, 2
