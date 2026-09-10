{{ config(materialized="table") }}

with
    date_spine as (
        -- La macro génère une colonne nommée 'date_day'
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2020-01-01' as date)",
                end_date="cast(TODAY() as date)",
            )
        }}
    )

select date_day
from date_spine
