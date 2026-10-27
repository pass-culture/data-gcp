with
    -- "Theoretical" spend convention, matching int_global__user_beneficiary's
    -- total_theoretical_remaining_credit: any non-cancelled booking reduces the
    -- available balance as of its creation date (reservation time), regardless
    -- of whether it has since been used/consumed.
    deposit_spend as (
        select
            deposit_id,
            date(booking_creation_date) as spend_day,
            sum(booking_intermediary_amount) as spend_amount
        from {{ ref("int_global__booking") }}
        where not booking_is_cancelled
        group by deposit_id, spend_day
    ),

    daily_deposit_with_spend as (
        select
            dud.deposit_active_date as pricing_day,
            dud.deposit_id,
            dud.deposit_amount,
            coalesce(ds.spend_amount, 0) as spend_on_day
        from {{ ref("int_global__daily_deposit") }} as dud
        left join
            deposit_spend as ds
            on dud.deposit_id = ds.deposit_id
            and dud.deposit_active_date = ds.spend_day
        -- partition filter required on deposit_active_date (BigQuery partition
        -- elimination)
        where dud.deposit_active_date > date("2021-01-01")
    ),

    -- Remaining balance per deposit, per day: granted amount minus the
    -- cumulative spend to date (not just today's spend) - a stock, not a flow.
    deposit_remaining_credit as (
        select
            pricing_day,
            deposit_id,
            deposit_amount - sum(spend_on_day) over (
                partition by deposit_id order by pricing_day
            ) as remaining_credit
        from daily_deposit_with_spend
    )

select
    pricing_day,
    -- Floor each deposit's balance at 0 before summing so one noisy/negative
    -- deposit (e.g. a data-quality edge case) cannot offset others.
    sum(greatest(remaining_credit, 0)) as total_available_credits
from deposit_remaining_credit
group by pricing_day
order by pricing_day
