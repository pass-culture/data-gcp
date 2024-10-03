{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
        )
    )
}}

with
    _rows as (
        select
            date(timestamp) as partition_date,
            timestamp,
            jsonpayload.message,
            jsonpayload.technical_message_id,
            cast(jsonpayload.user_id as string) as user_id,
            jsonpayload.extra.searchtype as search_type,
            coalesce(
                jsonpayload.extra.searchsubtype, lower(jsonpayload.extra.searchprotype)
            ) as search_protype,
            case
                when jsonpayload.extra.searchquery like "%@%"
                then "xxx@xxx.com"
                else jsonpayload.extra.searchquery
            end as search_query,
            cast(jsonpayload.extra.searchnbresults as int) as search_nb_results,
            cast(jsonpayload.extra.searchrank as int) as card_clicked_rank

        from {{ source("raw", "stdout") }}
        where
            1 = 1
            {% if is_incremental() %}
                and date(timestamp) >= date_sub(date('{{ ds() }}'), interval 7 day)
                and date(timestamp) <= date("{{ ds() }}")
            {% endif %}
            and jsonpayload.extra.analyticssource = 'backoffice'
    ),

    generate_session as (
        select
            *,
            rnk - session_sum as session_num,
            min(timestamp) over (
                partition by user_id, rnk - session_sum
            ) as session_start
        from
            (
                select
                    *,
                    sum(same_session) over (
                        partition by user_id order by timestamp
                    ) as session_sum,
                    row_number() over (partition by user_id order by timestamp) as rnk
                from
                    (
                        select
                            *,
                            coalesce(
                                cast(
                                    date_diff(
                                        timestamp,
                                        lag(timestamp, 1) over (
                                            partition by user_id order by timestamp
                                        ),
                                        minute
                                    )
                                    <= 30 as int
                                ),
                                1
                            ) as same_session,
                        from _rows
                    ) _inn_count
            ) _inn_ts
    )

select
    * except (session_num, session_start, rnk, same_session, session_sum),
    to_hex(
        md5(concat(cast(session_start as string), user_id, session_num))
    ) as session_id
from generate_session
{% if is_incremental() %} where partition_date = date("{{ ds() }}") {% endif %}
