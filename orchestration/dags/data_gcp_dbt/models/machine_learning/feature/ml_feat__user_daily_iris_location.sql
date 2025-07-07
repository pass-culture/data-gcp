{{
    config(
        materialized="table",
        tags=["weekly"],
        labels={"schedule": "weekly"},
    )
}}

with
    iris_data as (
        select id as iris_id, centroid from {{ ref("int_seed__iris_france") }}
    ),

    user_iris_data as (
        select distinct user_id, user_context.user_iris_id, event_date
        from {{ ref("int_pcreco__past_offer_context") }}
        where
            user_context.user_iris_id is not null
            and event_date >= date_sub(date('{{ ds() }}'), interval 60 day)
    ),

    date_series as (select distinct event_date from user_iris_data order by event_date),

    distinct_users as (select distinct user_id from user_iris_data),

    user_days as (
        select users.user_id, dates.event_date
        from distinct_users as users
        cross join date_series as dates
    ),

    -- Create a table of all possible user/date combinations with their closest future
    -- position
    future_positions as (
        select
            ud.user_id,
            ud.event_date,
            first_value(uid.user_iris_id) over (
                partition by ud.user_id, ud.event_date
                order by uid.event_date
                rows between unbounded preceding and unbounded following
            ) as future_iris_id,
            first_value(uid.event_date) over (
                partition by ud.user_id, ud.event_date
                order by uid.event_date
                rows between unbounded preceding and unbounded following
            ) as future_date
        from user_days as ud
        left join
            user_iris_data as uid
            on ud.user_id = uid.user_id
            and ud.event_date <= uid.event_date
    ),

    -- Create a table of all possible user/date combinations with their closest past
    -- position
    past_positions as (
        select
            ud.user_id,
            ud.event_date,
            first_value(uid.user_iris_id) over (
                partition by ud.user_id, ud.event_date
                order by uid.event_date desc
                rows between unbounded preceding and unbounded following
            ) as past_iris_id,
            first_value(uid.event_date) over (
                partition by ud.user_id, ud.event_date
                order by uid.event_date desc
                rows between unbounded preceding and unbounded following
            ) as past_date
        from user_days as ud
        left join
            user_iris_data as uid
            on ud.user_id = uid.user_id
            and ud.event_date > uid.event_date
    ),

    -- Deduplicate the future positions (take one row per user/date)
    distinct_future_positions as (
        select user_id, event_date, future_iris_id, future_date
        from future_positions
        group by user_id, event_date, future_iris_id, future_date
    ),

    -- Deduplicate the past positions (take one row per user/date)
    distinct_past_positions as (
        select user_id, event_date, past_iris_id, past_date
        from past_positions
        group by user_id, event_date, past_iris_id, past_date
    ),

    -- Combine positions, prioritizing:
    -- 1. Current day position (when future_date = event_date)
    -- 2. Past position
    -- 3. Future position (only when no past positions available)
    daily_positions as (
        select
            ud.user_id,
            ud.event_date,
            case
                when fp.future_date = ud.event_date
                then fp.future_iris_id  -- Same day (highest priority)
                when pp.past_iris_id is not null
                then pp.past_iris_id  -- Past position (second priority)
                else fp.future_iris_id  -- Future position (lowest priority)
            end as user_iris_id,
            case
                when fp.future_date = ud.event_date
                then fp.future_date  -- Same day
                when pp.past_iris_id is not null
                then pp.past_date  -- Past position
                else fp.future_date  -- Future position
            end as user_iris_source_date
        from user_days as ud
        left join
            distinct_future_positions as fp
            on ud.user_id = fp.user_id
            and ud.event_date = fp.event_date
        left join
            distinct_past_positions as pp
            on ud.user_id = pp.user_id
            and ud.event_date = pp.event_date
    )

select
    daily_positions.user_id,
    daily_positions.event_date,
    daily_positions.user_iris_id,
    daily_positions.user_iris_source_date,
    iris_data.centroid as user_centroid,
    st_x(iris_data.centroid) as user_centroid_x,
    st_y(iris_data.centroid) as user_centroid_y
from daily_positions
left join iris_data on daily_positions.user_iris_id = iris_data.iris_id
order by daily_positions.user_id, daily_positions.event_date
