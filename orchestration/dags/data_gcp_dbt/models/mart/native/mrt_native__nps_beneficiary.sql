with
    active_deposit_at_end_date as (
        select
            r.user_id,
            r.response_id,
            d.deposit_type,
            row_number() over (
                partition by r.user_id, r.response_id
                order by d.deposit_creation_date desc
            ) as rank
        from {{ ref("mrt_global__deposit") }} as d
        inner join
            {{ ref("int_qualtrics__nps_beneficiary_answer") }} as r
            on d.user_id = r.user_id
            and d.deposit_creation_date <= date(r.end_date)
    ),

    total_bookings_at_end_date as (
        select
            r.user_id,
            r.response_id,
            b.booking_rank as total_bookings,
            row_number() over (
                partition by r.user_id, r.response_id
                order by b.booking_creation_date desc
            ) as rank
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("int_qualtrics__nps_beneficiary_answer") }} as r
            on b.user_id = r.user_id
            and b.booking_creation_date <= date(r.end_date)
    )

select
    r.end_date as response_date,
    r.user_id,
    r.response_id,
    d.deposit_type,
    u.user_civility,
    u.user_region_name,
    u.user_activity,
    u.user_is_in_qpv,
    b.total_bookings,
    safe_cast(r.answer as int64) as response_rating,
    date_diff(r.end_date, u.user_activation_date, day) as user_seniority
from {{ ref("int_qualtrics__nps_beneficiary_answer") }} as r
inner join {{ ref("mrt_global__user") }} as u on r.user_id = u.user_id
left join
    active_deposit_at_end_date as d
    on r.user_id = d.user_id
    and r.response_id = d.response_id
    and d.rank = 1
left join
    total_bookings_at_end_date as b
    on r.user_id = b.user_id
    and r.response_id = b.response_id
    and b.rank = 1
where r.is_nps_question = true
