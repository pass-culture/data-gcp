{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

WITH adage_logs AS (
    SELECT
        partition_date as event_date,
        url_path,
        log_timestamp as event_timestamp,
        message as event_name,
        technical_message_id,
        source,
        extra_user_id AS user_id,
        uai,
        user_role,
        origin,
        stock_id as collective_stock_id,
        query_id,
        comment,
        requested_date,
        total_students,
        total_teachers,
        header_link_name,
        booking_id AS collective_booking_id,
        address_type_filter,
        text_filter,
        department_filter,
        academy_filter,
        venue_filter,
        artistic_domain_filter,
        student_filter,
        format_filter,
        suggestion_type,
        suggestion_value,
        is_favorite,
        playlist_id,
        domain_id,
        venue_id,
        rank_clicked,
        CASE
            WHEN url_path LIKE "%adage-iframe%" THEN 'adage-iframe'
            ELSE 'adage'
        END as log_source,
        CASE WHEN message="CreateCollectiveOfferRequest" THEN collective_offer_template_id ELSE offer_id END as collective_offer_id,
        CASE WHEN message="SearchButtonClicked" THEN results_count WHEN message="TrackingFilter" THEN results_number ELSE NULL END as total_results,
        COALESCE(
                CAST(
                    DATE_DIFF(log_timestamp, LAG(log_timestamp, 1) OVER (PARTITION BY user_id ORDER BY log_timestamp), MINUTE) <= 30 AS INT
                ), 1
            ) as same_session
    FROM {{ref('int_pcapi__log')}}
    WHERE
        (
            url_path LIKE "%adage-iframe%"
            OR analytics_source = 'adage'
        )
    AND message NOT LIKE "%HTTP%"
    {% if is_incremental() %}
    AND partition_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
    {% endif %}
),

generate_session AS (
    SELECT
        *,
        rnk - session_sum  as session_num,
        MIN(event_timestamp) OVER (PARTITION BY user_id, rnk - session_sum) as session_start
    FROM (
        SELECT
        *,
        SUM(same_session) OVER (PARTITION BY user_id  ORDER BY event_timestamp) as session_sum,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp) as rnk
        FROM adage_logs
    ) _inn_ts
)

SELECT
* EXCEPT(session_num, session_start, rnk, same_session, session_sum),
TO_HEX(MD5(CONCAT(CAST(session_start AS STRING), user_id, session_num))) as session_id
FROM generate_session
{% if is_incremental() %}
WHERE event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
{% endif %}
