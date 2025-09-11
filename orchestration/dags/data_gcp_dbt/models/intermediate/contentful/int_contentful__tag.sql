select
    tag_id,
    {{ clean_str("tag_name") }} as tag_name,
    entry_id,
    tag_key,
    tag_value,
    case when tag_key = "pl" then tag_value end as playlist_type
from
    (
        select
            tag_id,
            tag_name,
            entry_id,
            execution_date,
            split(tag_name, ':')[safe_ordinal(1)] as tag_key,
            split(tag_name, ':')[safe_ordinal(2)] as tag_value,
            row_number() over (
                partition by tag_id, entry_id order by execution_date desc
            ) as row_number
        from {{ source("raw", "contentful_tag") }}
    ) inn
where row_number = 1
