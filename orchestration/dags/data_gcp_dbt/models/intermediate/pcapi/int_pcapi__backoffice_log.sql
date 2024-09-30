{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'partition_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns"
    )
) }}

WITH backoffice_logs AS (
SELECT
    partition_date,
    log_timestamp,
    message,
    technical_message_id,
    user_id,
    search_type,
    COALESCE(search_sub_type,LOWER(search_pro_type)) as search_protype,
    CASE WHEN search_query LIKE "%@%" THEN "xxx@xxx.com" ELSE search_query END as search_query,
    search_nb_results,
    card_clicked_rank

FROM
   {{ ref("int_pcapi__log") }}
WHERE
    1 = 1
    {% if is_incremental() %}
    AND DATE(log_timestamp) >= DATE_SUB(DATE('{{ ds() }}'), interval 7 day)
    AND DATE(log_timestamp) <= DATE("{{ ds() }}")
    {% endif %}
    AND analytics_source = 'backoffice'
),

generate_session AS (
    SELECT
        *,
        rnk - session_sum  as session_num,
        MIN(log_timestamp) OVER (PARTITION BY user_id, rnk - session_sum) as session_start
    FROM (
        SELECT
        *,
        SUM(same_session) OVER (PARTITION BY user_id  ORDER BY log_timestamp) as session_sum,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY log_timestamp) as rnk
        FROM (
            SELECT
            *,
            COALESCE(
                CAST(
                    DATE_DIFF(log_timestamp, LAG(log_timestamp, 1) OVER (PARTITION BY user_id ORDER BY log_timestamp), MINUTE) <= 30 AS INT
                ), 1
            ) as same_session,
            FROM backoffice_logs
        ) _inn_count
    ) _inn_ts
)

SELECT
* EXCEPT(session_num, session_start, rnk, same_session, session_sum),
TO_HEX(MD5(CONCAT(CAST(session_start AS STRING), user_id, session_num))) as session_id
FROM generate_session
{% if is_incremental() %}
WHERE partition_date = DATE("{{ ds() }}")
{% endif %}
