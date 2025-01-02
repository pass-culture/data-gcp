{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
            require_partition_filter=true,
        )
    )
}}

with
    backoffice_logs as (
        select
            partition_date,
            log_timestamp,
            technical_message_id,
            user_id,
            search_type,
            search_nb_results,
            card_clicked_rank,
            case
                when message like 'HTTP request%' then REGEXP_REPLACE(REGEXP_REPLACE(message, r'HTTP request at /', ''), r'/\d+', '')
                else message
                end as message,
            COALESCE(search_sub_type, LOWER(search_pro_type)) as search_protype,
            case
                when search_query like '%@%' then 'xxx@xxx.com' else search_query
            end as search_query

        from {{ ref("int_pcapi__log") }}
        where
            log_timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), interval 365 day)

            {% if is_incremental() %}
                and DATE(log_timestamp) >= DATE_SUB(DATE('{{ ds() }}'), interval 2 day)
                and DATE(log_timestamp) <= DATE('{{ ds() }}')
            {% endif %}
            and (
            (analytics_source = 'backoffice')
            or (k8s_pod_role = 'backoffice'
            and message not like 'HTTP request at /(health/api)%'
            and message not like 'HTTP request at /static%')
            )
    ),

    generate_session as (
        select
            *,
            rnk - session_sum as session_num,
            MIN(log_timestamp) over (
                partition by user_id, rnk - session_sum
            ) as session_start
        from
            (
                select
                    *,
                    SUM(same_session) over (
                        partition by user_id order by log_timestamp
                    ) as session_sum,
                    ROW_NUMBER() over (
                        partition by user_id order by log_timestamp
                    ) as rnk
                from
                    (
                        select
                            *,
                            COALESCE(
                                CAST(
                                    DATE_DIFF(
                                        log_timestamp,
                                        LAG(log_timestamp, 1) over (
                                            partition by user_id order by log_timestamp
                                        ),
                                        minute
                                    )
                                    <= 30 as int
                                ),
                                1
                            ) as same_session
                        from backoffice_logs
                    ) as _inn_count
            ) as _inn_ts
    )

select
    * except (session_num, session_start, rnk, same_session, session_sum),
    TO_HEX(
        MD5(CONCAT(CAST(session_start as string), user_id, session_num))
    ) as session_id
from generate_session
{% if is_incremental() %} where partition_date = DATE('{{ ds() }}') {% endif %}
