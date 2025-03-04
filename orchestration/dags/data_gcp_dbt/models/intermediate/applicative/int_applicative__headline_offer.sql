with
    headline_offer_flattened as (
        select
            offer_id,
            venue_id,
            case
                when
                    safe.parse_timestamp(
                        '%Y-%m-%d %H:%M:%E6S', regexp_extract(timespan, r',"([^"]+)"')
                    )
                    is null
                then true
                else false
            end as is_headlined,
            safe.parse_timestamp(
                '%Y-%m-%d %H:%M:%E6S', regexp_extract(timespan, r'"([^"]+)"')
            ) as headline_beginning_time,
            safe.parse_timestamp(
                '%Y-%m-%d %H:%M:%E6S', regexp_extract(timespan, r',"([^"]+)"')
            ) as headline_ending_time
        from {{ source("raw", "applicative_database_headline_offer") }}
    )

select distinct
    offer_id,
    count(offer_id) over (partition by offer_id) as total_headlines,
    max(is_headlined) over (partition by offer_id) as is_headlined,
    min(headline_beginning_time) over (partition by offer_id) as first_headline_date,
    first_value(headline_ending_time respect nulls) over (
        partition by offer_id order by headline_ending_time asc
    ) as last_headline_date
from headline_offer_flattened
