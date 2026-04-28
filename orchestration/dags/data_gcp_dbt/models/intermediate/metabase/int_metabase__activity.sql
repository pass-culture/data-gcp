with
    public_collections as (
        select
            collection_id,
            collection_name,
            location,
            concat(location, collection_id, '/') as full_path
        from {{ source("raw", "metabase_collection") }}
        where personal_owner_id is null
    ),

    collection_ancestor_ids as (
        select
            c.collection_id as leaf_collection_id,
            cast(ancestor_id as int64) as ancestor_collection_id,
            ord
        from
            public_collections as c,
            unnest(split(trim(c.full_path, '/'), '/')) as ancestor_id
        with
        offset as ord
        where ancestor_id != ''
    ),

    collection_ancestor_names as (
        select
            a.leaf_collection_id as collection_id,
            array_agg(
                lower(c.collection_name) order by a.ord
            ) as ancestor_collection_names
        from collection_ancestor_ids as a
        inner join public_collections as c on a.ancestor_collection_id = c.collection_id
        group by 1
    )

select
    rank_execution.card_id,
    rank_execution.card_name,
    rank_execution.card_creation_date,
    rank_execution.card_update_date,
    rank_execution.card_collection_id,
    rank_execution.context as last_execution_context,
    public_collections.collection_name as card_collection_name,
    public_collections.location as card_collection_location,
    aggregated_activity.avg_running_time,
    aggregated_activity.avg_result_rows,
    aggregated_activity.total_users,
    aggregated_activity.total_views,
    aggregated_activity.total_views_3_months,
    aggregated_activity.total_views_6_months,
    aggregated_activity.nbr_dashboards,
    aggregated_activity.last_execution_date,
    aggregated_activity.total_errors,
    coalesce(
        collection_ancestor_names.ancestor_collection_names, []
    ) as ancestor_collection_names,
    date_diff(
        current_date(), date(aggregated_activity.last_execution_date), day
    ) as days_since_last_execution
from {{ ref("int_metabase__daily_query") }} as rank_execution
inner join
    {{ ref("int_metabase__aggregated_card_activity") }} as aggregated_activity
    on rank_execution.card_id = aggregated_activity.card_id
inner join
    public_collections
    on rank_execution.card_collection_id = public_collections.collection_id
left join
    collection_ancestor_names
    on rank_execution.card_collection_id = collection_ancestor_names.collection_id
where rank_execution.card_id_execution_rank = 1
