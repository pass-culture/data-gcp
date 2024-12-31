select
    ref_archive.*,
    rank_execution.card_id,
    rank_execution.card_name,
    rank_execution.card_creation_date,
    rank_execution.card_update_date,
    rank_execution.card_collection_id,
    location,
    avg_running_time,
    avg_result_rows,
    total_users,
    total_views,
    nbr_dashboards,
    last_execution_date,
    context as last_execution_context,
    total_errors,
    case
        when location like '/610%' or card_collection_id = 610
        then 'archive'
        when location like '/607%'
        then 'externe'
        when location like '/606/614%' or card_collection_id = 614
        then 'operationnel'
        when location like '/606/615%' or card_collection_id = 615
        then 'adhoc'
        when location like '/608%'
        then 'interne'
        when location like '/1783%'
        then 'restreint'
        else 'other'
    end as parent_folder
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
