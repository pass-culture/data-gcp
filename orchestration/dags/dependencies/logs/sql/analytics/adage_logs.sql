WITH _rows AS (
SELECT
    DATE(timestamp) as partition_date,
    CASE 
        WHEN jsonPayload.extra.path LIKE "%adage-iframe%" THEN 'adage-iframe'
        ELSE 'adage'
    END as log_source,
    timestamp,
    jsonPayload.extra.path AS path,
    jsonPayload.extra.statuscode AS status_code,
    jsonPayload.extra.method AS method,
    jsonPayload.extra.sourceip AS source_ip,
    jsonPayload.extra.duration AS duration,
    jsonPayload.extra.source as source,
    jsonPayload.extra.userId as user_id,
    jsonPayload.extra.stockId as stock_id,
    coalesce(jsonPayload.extra.bookingId, jsonPayload.extra.booking_id) as booking_id,
FROM
    `{{ bigquery_raw_dataset }}.stdout`
WHERE
    DATE(timestamp) >= DATE("{{ add_days(ds, -7) }}")
    AND DATE(timestamp) <= DATE("{{ ds }}")
    AND (
        jsonPayload.extra.path LIKE "%adage-iframe%"
        OR jsonPayload.extra.analyticsSource = 'adage'
    )
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
TO_HEX(MD5(CONCAT(CAST(session_start AS STRING), user_id, session_num))) as session_id,
FROM generate_session