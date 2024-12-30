WITH collection_status AS (
  SELECT
    collection_id,
    collection_name
  FROM {{ source("raw", "metabase_collection") }}
  WHERE
    -- the collection is not located in Temporary Archive directory.
    concat(location, collection_id) NOT LIKE '/610%'
    -- the collection is not located in Metabase's native archive.
    AND archived = false
    -- the collections are public (i.e., remove personal collections)
    AND personal_owner_id IS null
)


SELECT
    collection_id AS metabase_collection_id,
    collection_name AS metabase_collection_name,
    count(DISTINCT card_id) AS total_metabase_cards,
    count(DISTINCT metabase_user_id) AS total_metabase_users,
    count(*) AS total_metabase_queries
    FROM {{ ref("int_metabase__daily_query") }} AS dq
    INNER JOIN collection_status AS cs ON dq.card_collection_id = cs.collection_id
    WHERE date(execution_date) >= date_sub(current_date, INTERVAL 90 DAY )
GROUP BY 1,2
