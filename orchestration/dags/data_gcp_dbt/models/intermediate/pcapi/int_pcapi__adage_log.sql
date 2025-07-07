{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with
    adage_logs as (
        select
            partition_date as event_date,
            url_path,
            log_timestamp as event_timestamp,
            message as event_name,
            technical_message_id,
            source,
            extra_user_id as user_id,
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
            booking_id as collective_booking_id,
            address_type_filter,
            text_filter,
            department_filter,
            academy_filter,
            geoloc_radius_filter,
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
            case
                when url_path like "%adage-iframe%" then 'adage-iframe' else 'adage'
            end as log_source,
            case
                when message = "CreateCollectiveOfferRequest"
                then collective_offer_template_id
                else offer_id
            end as collective_offer_id,
            case
                when message = "SearchButtonClicked"
                then results_count
                when message = "TrackingFilter"
                then results_number
                else null
            end as total_results,
            coalesce(
                cast(
                    date_diff(
                        log_timestamp,
                        lag(log_timestamp, 1) over (
                            partition by user_id order by log_timestamp
                        ),
                        minute
                    )
                    <= 30 as int
                ),
                1
            ) as same_session
        from {{ ref("int_pcapi__log") }}
        where
            log_timestamp >= date_sub(current_timestamp(), interval 365 day)
            and (url_path like "%adage-iframe%" or analytics_source = 'adage')
            and message not like "%HTTP%"
            {% if is_incremental() %}
                and partition_date
                between date_sub(date("{{ ds() }}"), interval 2 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}
    ),

    generate_session as (
        select
            *,
            rnk - session_sum as session_num,
            min(event_timestamp) over (
                partition by user_id, rnk - session_sum
            ) as session_start
        from
            (
                select
                    *,
                    sum(same_session) over (
                        partition by user_id order by event_timestamp
                    ) as session_sum,
                    row_number() over (
                        partition by user_id order by event_timestamp
                    ) as rnk
                from adage_logs
            ) _inn_ts
    )

select
    * except (session_num, session_start, rnk, same_session, session_sum),
    to_hex(
        md5(concat(cast(session_start as string), user_id, session_num))
    ) as session_id
from generate_session
{% if is_incremental() %}
    where
        event_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
{% endif %}
