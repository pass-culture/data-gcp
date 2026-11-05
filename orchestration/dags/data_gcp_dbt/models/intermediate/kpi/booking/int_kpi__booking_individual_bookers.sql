{#
    New bookers by month: a beneficiary is counted once, on the month of his
    first used booking since the start of the reporting history (2022), and
    once per category, on the month of his first used booking in that category.
    A beneficiary has a single geography, so new bookers add up across
    territories and months: distinct bookers of any territory over any period
    starting in 2022 are the sum of the new bookers.
#}
{% set history_start_date = "2022-01-01" %}

with
    first_category_bookings as (
        select
            b.user_id,
            b.offer_category_id,
            u.user_region_name as region_name,
            u.user_department_name as department_name,
            u.user_epci_code as epci_code,
            u.user_municipality_code as municipality_code,
            min(b.booking_used_date) as first_used_date
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
        where
            b.booking_is_used
            and date(b.booking_used_date) >= date('{{ history_start_date }}')
        group by
            b.user_id,
            b.offer_category_id,
            u.user_region_name,
            u.user_department_name,
            u.user_epci_code,
            u.user_municipality_code
    ),

    -- the first booking of the beneficiary, all categories, is the earliest
    -- first booking among his categories
    flagged as (
        select
            *,
            row_number() over (
                partition by user_id order by first_used_date, offer_category_id
            )
            = 1 as is_first_booking
        from first_category_bookings
    )

select
    region_name,
    department_name,
    epci_code,
    municipality_code,
    offer_category_id,
    date_trunc(date(first_used_date), month) as partition_month,
    countif(is_first_booking) as total_new_bookers,
    count(*) as total_new_category_bookers
from flagged
group by
    partition_month,
    region_name,
    department_name,
    epci_code,
    municipality_code,
    offer_category_id
