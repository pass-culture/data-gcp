WITH public_collections as (
    -- Get public collections not in archive
    SELECT * 
    FROM  `{{ bigquery_raw_dataset }}.metabase_collections`
    WHERE personal_owner_id is null
    AND location not like '/610%' 
    AND collection_id != 610
),

base as (
    SELECT 
        card_id
        , dashboard_id
        , context
        , report_card.card_name as card_name
        , report_card.created_at as card_creation_date
        , report_card.updated_at as card_update_date
        , report_card.card_collection_id as card_collection_id
        , location
        , report_dashboard.dashboard_name as dashboard_name
        , min(report_dashboard.created_at) as dashboard_creation_date
        , max(report_dashboard.updated_at) as dashboard_update_date
        , avg(running_time) as avg_running_time
        , avg(result_rows) as avg_result_rows
        , count(distinct executor_id) as total_users
        , count(distinct execution_query.execution_id) as total_views
        , max(execution_query.execution_date) as last_execution_date
        , sum(case when error is null then 0 else 1 end) as total_errors
        -- , report_dashboard.collection_id as dashboard_collection_id
    FROM `{{ bigquery_raw_dataset }}.metabase_query_execution` as execution_query
    JOIN `{{ bigquery_raw_dataset }}.metabase_report_card` as report_card
        ON execution_query.card_id = report_card.id
    LEFT JOIN `{{ bigquery_raw_dataset }}.metabase_report_dashboard` as report_dashboard
        ON execution_query.dashboard_id = report_dashboard.id
    JOIN public_collections
        ON public_collections.collection_id = report_card.card_collection_id
    GROUP BY
        card_id
        , context
        , card_name
        , card_creation_date
        , card_update_date
        , card_collection_id
        , location
        , dashboard_id
        , dashboard_name
        ),

get_mates1 as (
    SELECT DISTINCT 
        card_id
        , dashboard_id
    FROM `{{ bigquery_raw_dataset }}.metabase_query_execution` as execution_query
    WHERE dashboard_id is not null
),

get_mates2 as (
    SELECT DISTINCT 
        card_id
        , dashboard_id
    FROM `{{ bigquery_raw_dataset }}.metabase_query_execution` as execution_query
    WHERE dashboard_id is not null
),

mates as (
    SELECT
        get_mates1.card_id as card_id
        , get_mates2.card_id as mate_card_id
        , get_mates1.dashboard_id as dashboard_id
    FROM get_mates1
    CROSS JOIN get_mates2
    WHERE get_mates1.dashboard_id = get_mates2.dashboard_id
    AND get_mates1.card_id != get_mates2.card_id
    ),

mates_data as (
    SELECT DISTINCT
        mates.card_id
        , context
        , mates.dashboard_id
        , count(distinct mate_card_id) over(partition by mates.card_id, mates.dashboard_id) as nbr_mates
        , SUM(CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), date(last_execution_date), day) > 100 
            THEN 1 
            ELSE 0 
            END) over(partition by mates.card_id, context, mates.dashboard_id) as nbr_mates_inactive
    FROM mates
    LEFT JOIN base
        ON mates.mate_card_id = base.card_id 
        AND mates.dashboard_id = base.dashboard_id
        ),

inactivity_mates as (
    SELECT 
        card_id
        , context
        , dashboard_id
        , CASE
            WHEN nbr_mates = nbr_mates_inactive 
            THEN 1
            ELSE 0
        END as all_mates_inactive
    FROM mates_data
    )

SELECT 
    base.card_id
    , base.dashboard_id
    , base.context
    , base.card_name
    , base.card_collection_id
    , location
    , base.dashboard_name
    , avg_running_time
    , avg_result_rows
    , total_users
    , total_views
    , total_errors
    , last_execution_date
    , all_mates_inactive
    , CASE
        WHEN DATE_DIFF(CURRENT_DATE(), date(last_execution_date), day) > 100
        THEN 1
        ELSE 0
    END as inactive
    , CASE
        WHEN lower(card_name) like '%archive%'
        THEN 1
        ELSE 0
    END as card_contains_archive
    , ref_archive.*
FROM base
LEFT JOIN inactivity_mates
    ON base.card_id = inactivity_mates.card_id
    AND base.context = inactivity_mates.context
LEFT JOIN `{{ bigquery_analytics_dataset }}.metabase_ref_collections_archive` as ref_archive
    ON base.card_collection_id = ref_archive.collection_id