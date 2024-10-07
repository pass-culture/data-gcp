{{ config(timeout=2400) }}
with
    all_days_since_activation as (
        select distinct
            offerer_id,
            offerer_creation_date,
            date_add(date(offerer_creation_date), interval offset day) as day
        from {{ ref("mrt_global__offerer") }}
        inner join
            {{ ref("aggregated_daily_offer_consultation_data") }} using (offerer_id)
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2018-01-01', day))) as
        offset
        where
            date_add(date(offerer_creation_date), interval offset day)
            >= offerer_creation_date  -- Les jours depuis la création de la structure
            and date_add(date(offerer_creation_date), interval offset day)
            <= current_date()  -- Que des jours avant aujourd'hui
    ),

    consult_per_offerer_and_day as (
        select event_date, offerer_id, sum(cnt_events) as nb_daily_consult
        from {{ ref("aggregated_daily_offer_consultation_data") }}
        where event_name = 'ConsultOffer'
        group by 1, 2
    ),

    cum_consult_per_day as (
        select
            *,
            sum(nb_daily_consult) over (
                partition by offerer_id order by event_date
            ) as cum_consult
        from consult_per_offerer_and_day
    )

select
    day as event_date,
    date_diff(current_date, day, day) as day_seniority,
    all_days_since_activation.offerer_id,
    coalesce(nb_daily_consult, 0) as nb_daily_consult,
    max(cum_consult) over (
        partition by all_days_since_activation.offerer_id
        order by day
        rows unbounded preceding
    ) as cum_consult
from all_days_since_activation
left join
    cum_consult_per_day
    on all_days_since_activation.offerer_id = cum_consult_per_day.offerer_id
    and all_days_since_activation.day = cum_consult_per_day.event_date
where date_diff(current_date, day, day) <= 180
