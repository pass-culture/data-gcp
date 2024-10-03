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
            case
                when jsonpayload.extra.path like "%adage-iframe%"
                then 'adage-iframe'
                else 'adage'
            end as log_source,
            timestamp,

            jsonpayload.message,
            jsonpayload.technical_message_id,
            jsonpayload.extra.source as source,
            jsonpayload.extra.userid as user_id,
            jsonpayload.extra.uai as uai,
            jsonpayload.extra.user_role as user_role,
            jsonpayload.extra.from as origin,
            cast(jsonpayload.extra.stockid as string) as stock_id,
            cast(jsonpayload.extra.offerid as string) as offer_id,
            cast(
                jsonpayload.extra.collective_offer_template_id as string
            ) as collective_offer_template_id,
            cast(jsonpayload.extra.queryid as string) as query_id,
            jsonpayload.extra.comment as comment,
            jsonpayload.extra.requested_date as requested_date,
            cast(jsonpayload.extra.total_students as int) as total_students,
            cast(jsonpayload.extra.total_teachers as int) as total_teachers,
            jsonpayload.extra.header_link_name as header_link_name,
            cast(
                coalesce(
                    jsonpayload.extra.bookingid, jsonpayload.extra.booking_id
                ) as string
            ) as booking_id,
            array_to_string(jsonpayload.extra.filters, ',') as filters,
            case
                when jsonpayload.message = "SearchButtonClicked"
                then cast(jsonpayload.extra.resultscount as int)
                when jsonpayload.message = "TrackingFilter"
                then cast(jsonpayload.extra.resultnumber as int)
                else null
            end as results_count,
            jsonpayload.extra.filtervalues.eventaddresstype as address_type_filter,
            cast(jsonpayload.extra.filtervalues.query as string) as text_filter,
            array_to_string(
                jsonpayload.extra.filtervalues.departments, ','
            ) as department_filter,
            array_to_string(
                jsonpayload.extra.filtervalues.academies, ','
            ) as academy_filter,
            array_to_string(
                array(
                    select cast(value as string)
                    from unnest(jsonpayload.extra.filtervalues.venue) as value
                ),
                ','
            ) as venue_filter,
            array_to_string(
                array(
                    select cast(value as string)
                    from unnest(jsonpayload.extra.filtervalues.domains) as value
                ),
                ','
            ) as artistic_domain_filter,
            array_to_string(
                array(
                    select cast(value as string)
                    from unnest(jsonpayload.extra.filtervalues.students) as value
                ),
                ','
            ) as student_filter,
            array_to_string(
                jsonpayload.extra.filtervalues.formats, ','
            ) as format_filter,
            array_to_string(
                jsonpayload.extra.filtervalues.categories, ','
            ) as category_filter,
            jsonpayload.extra.suggestiontype as suggestion_type,
            jsonpayload.extra.suggestionvalue as suggestion_value,
            cast(jsonpayload.extra.isfavorite as boolean) as is_favorite,
            cast(cast(jsonpayload.extra.playlistid as int) as string) as playlist_id,
            cast(cast(jsonpayload.extra.domainid as int) as string) as domain_id,
            cast(cast(jsonpayload.extra.venueid as int) as string) as venue_id,
            cast(jsonpayload.extra.index as int) as rank_clicked

        from {{ source("raw", "stdout") }}
        where
            1 = 1
            {% if is_incremental() %}
                and date(timestamp) >= date_sub(date('{{ ds() }}'), interval 7 day)
                and date(timestamp) <= date("{{ ds() }}")
            {% endif %}
            and (
                jsonpayload.extra.path like "%adage-iframe%"
                or jsonpayload.extra.analyticssource = 'adage'
            )
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
