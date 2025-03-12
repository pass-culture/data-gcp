{{
    config(
        materialized="table",
    )
}}

with user_iris_data as (
    select
        user_id,
        user_context.user_iris_id,
        event_date,
        row_number() over(partition by user_id order by event_date desc) as row_num
    from {{ ref('int_pcreco__past_offer_context') }}
    where user_context.user_iris_id is not null and event_date >= date_sub(date('{{ ds() }}'), interval 60 day)
)

select
    user_id,
    user_iris_id as last_known_user_iris_id,
    event_date as last_known_user_iris_date
from user_iris_data
where row_num = 1
