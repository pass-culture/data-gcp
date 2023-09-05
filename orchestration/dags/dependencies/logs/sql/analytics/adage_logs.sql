WITH _rows AS (
SELECT
    DATE(timestamp) as partition_date,
    CASE 
        WHEN jsonPayload.extra.path LIKE "%adage-iframe%" THEN 'adage-iframe'
        ELSE 'adage'
    END as log_source,
    timestamp,

    jsonPayload.message,
    jsonPayload.technical_message_id,
    jsonPayload.extra.source as source,
    jsonPayload.extra.userId as user_id,
    jsonPayload.extra.uai AS uai,
    jsonPayload.extra.user_role AS user_role,
    jsonPayload.extra.AdageHeaderFrom as origin,
    cast(jsonPayload.extra.stockId as string) as stock_id,
    cast(jsonPayload.extra.offerId as string) as offer_id,
    cast(jsonPayload.extra.collective_offer_template_id as string) as collective_offer_template_id,
    cast(jsonPayload.extra.queryid as string) as query_id,
    jsonPayload.extra.comment as comment,
    jsonPayload.extra.requested_date as requested_date,
    cast(jsonPayload.extra.total_students as int) as total_students,
    cast(jsonPayload.extra.total_teachers as int) as total_teachers,
    jsonPayload.extra.header_link_name as header_link_name,
    CAST(coalesce(jsonPayload.extra.bookingId, jsonPayload.extra.booking_id) as string) as booking_id,
    ARRAY_TO_STRING(jsonPayload.extra.filters, ',') AS filters,
    cast(jsonPayload.extra.resultscount as int) as results_count,
    jsonPayload.extra.filtervalues.eventaddresstype as address_type_filter,
    jsonPayload.extra.filtervalues.query as text_filter,
    jsonPayload.extra.filtervalues.departments as department_filter,
    jsonPayload.extra.filtervalues.academies as academy_filter,

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
WHERE partition_date = DATE("{{ ds }}")
