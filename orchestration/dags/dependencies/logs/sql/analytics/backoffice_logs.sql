WITH _rows AS (
SELECT
    DATE(timestamp) as partition_date,
    timestamp,
    jsonPayload.message,
    jsonPayload.technical_message_id,
    cast(jsonPayload.user_id as string) as user_id,
    jsonPayload.extra.searchtype as search_type,
    COALESCE(jsonPayload.extra.searchsubtype,LOWER(jsonPayload.extra.searchprotype)) as search_protype,
    CASE WHEN jsonPayload.extra.searchquery LIKE "%@%" THEN "xxx@xxx.com" ELSE jsonPayload.extra.searchquery as search_query,
    cast(jsonPayload.extra.searchnbresults as int) as search_nb_results,
    cast(jsonPayload.extra.searchrank as int) as card_clicked_rank

FROM
    `{{ bigquery_raw_dataset }}.stdout`
WHERE
    DATE(timestamp) >= DATE("{{ add_days(ds, -7) }}")
    AND DATE(timestamp) <= DATE("{{ ds }}")
    AND jsonPayload.extra.analyticsSource = 'backoffice'
),

generate_session AS (
    SELECT 
        *,
        rnk - session_sum  as session_num,
        MIN(timestamp) OVER (PARTITION BY user_id, rnk - session_sum) as session_start
    FROM (
        SELECT
        *,
        SUM(same_session) OVER (PARTITION BY user_id  ORDER BY timestamp) as session_sum,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) as rnk
        FROM (
            SELECT 
            *,
            COALESCE(
                CAST(
                    DATE_DIFF(timestamp, LAG(timestamp, 1) OVER (PARTITION BY user_id ORDER BY timestamp), MINUTE) <= 30 AS INT
                ), 1
            ) as same_session,
            FROM _rows
        ) _inn_count
    ) _inn_ts
)

SELECT 
* EXCEPT(session_num, session_start, rnk, same_session, session_sum),
TO_HEX(MD5(CONCAT(CAST(session_start AS STRING), user_id, session_num))) as session_id
FROM generate_session
WHERE partition_date = DATE("{{ ds }}")