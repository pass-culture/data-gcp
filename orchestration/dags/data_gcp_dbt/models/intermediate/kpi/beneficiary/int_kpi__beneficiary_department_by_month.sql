{{ config(materialized="table") }}

-- Historizes department_code per (user_id, month) instead of using the
-- user's current (always-latest) address. int_global__user_beneficiary /
-- int_geo__user_location only carry the CURRENT address, so a beneficiary
-- who moves after activating a credit gets reattributed to their new
-- department for their ENTIRE deposit history, retroactively, every time
-- this pipeline runs -- e.g. someone who activates GRANT_18 in a rural
-- department and later moves to a university city shows up as having
-- always lived there. This model instead resolves, for each month a
-- beneficiary had an active deposit, the address actually on file at the
-- end of that month.
--
-- department_code is derived from the postal code (first 2 digits, 3 for
-- DOM/COM) rather than user_department_name, which is more direct to match
-- against region_department.num_dep downstream. Corse 2A/2B is not split
-- (kept as "20") -- postal code alone doesn't disambiguate it and none of
-- the current coverage KPIs need that split.
with
    active_months as (
        select distinct
            user_id, date_trunc(deposit_active_date, month) as reference_month
        from {{ ref("int_global__daily_deposit") }}
        where deposit_active_date > date_sub(current_date(), interval 48 month)
    ),

    history as (
        select
            user_id,
            user_information_rank,
            date(user_information_created_at) as user_information_created_at,
            case
                when substr(user_postal_code, 1, 2) in ('97', '98')
                then substr(user_postal_code, 1, 3)
                else substr(user_postal_code, 1, 2)
            end as department_code
        from {{ ref("mrt_global__user_beneficiary_information_history") }}
        where user_postal_code is not null and length(user_postal_code) = 5
    ),

    department_as_of_month as (
        select am.user_id, am.reference_month, h.department_code
        from active_months as am
        left join
            history as h
            on am.user_id = h.user_id
            and h.user_information_created_at <= last_day(am.reference_month)
        qualify
            row_number() over (
                partition by am.user_id, am.reference_month
                order by h.user_information_created_at desc
            )
            = 1
    ),

    -- Fallback for months before the user's first known address record
    -- (e.g. a deposit month that precedes their earliest history row) --
    -- use their earliest known address rather than leaving it null.
    department_first_known as (
        select user_id, department_code
        from history
        qualify
            row_number() over (partition by user_id order by user_information_rank asc)
            = 1
    )

select
    am.user_id,
    am.reference_month,
    -- "-1" sentinel matches int_geo__user_location's convention for "no
    -- known address" rather than surfacing a bare null here.
    coalesce(am.department_code, fk.department_code, '-1') as department_code
from department_as_of_month as am
left join department_first_known as fk on am.user_id = fk.user_id
