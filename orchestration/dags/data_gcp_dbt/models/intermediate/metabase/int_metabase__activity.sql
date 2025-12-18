select
    ref_archive.*,
    rank_execution.card_id,
    rank_execution.card_name,
    rank_execution.card_creation_date,
    rank_execution.card_update_date,
    rank_execution.card_collection_id,
    public_collections.location,
    aggregated_activity.avg_running_time,
    aggregated_activity.avg_result_rows,
    aggregated_activity.total_users,
    aggregated_activity.total_views,
    aggregated_activity.total_views_6_months,
    aggregated_activity.nbr_dashboards,
    aggregated_activity.last_execution_date,
    rank_execution.context as last_execution_context,
    aggregated_activity.total_errors,
    date_diff(
        current_date(), date(aggregated_activity.last_execution_date), day
    ) as days_since_last_execution,
    case
        when
            public_collections.location like '/610%'
            or rank_execution.card_collection_id = 610
        then 'archive'
        when public_collections.location like '/607%'
        then 'externe'
        when
            public_collections.location like '/606/614%'
            or rank_execution.card_collection_id = 614
        then 'operationnel'
        when
            public_collections.location like '/606/615%'
            or rank_execution.card_collection_id = 615
        then 'adhoc'
        when public_collections.location like '/608%'
        then 'interne'
        when public_collections.location like '/1783%'
        then 'restreint'
        else 'other'
    end as parent_folder,
    coalesce(
        lower(public_collections.collection_name) like '%thématique%', false
    ) as is_thematic_collection
from {{ ref("int_metabase__daily_query") }} as rank_execution
inner join
    {{ ref("int_metabase__aggregated_card_activity") }} as aggregated_activity
    on rank_execution.card_id = aggregated_activity.card_id
inner join
    {{ source("raw", "metabase_collection") }} as public_collections
    on rank_execution.card_collection_id = public_collections.collection_id
inner join
    {{ ref("int_metabase__collection_archive") }} as ref_archive
    on rank_execution.card_collection_id = ref_archive.collection_id
where rank_execution.card_id_execution_rank = 1
