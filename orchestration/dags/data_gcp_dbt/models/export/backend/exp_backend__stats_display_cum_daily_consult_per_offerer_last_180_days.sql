
{{ config(timeout=2400) }}
with
all_days_since_activation as (
    select distinct
        offerer_id,
        offerer_creation_date,
        DATE_ADD(
            DATE(offerer_creation_date), interval
            offset
            day
        ) as day
    from
        {{ ref('mrt_global__offerer') }}
        inner join
            {{ ref('aggregated_daily_offer_consultation_data') }}
            using
                (offerer_id)
        cross join
            UNNEST(
                GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), '2018-01-01', day))) as
            offset
    where
        DATE_ADD(
            DATE(offerer_creation_date), interval
            offset
            day
        ) >= offerer_creation_date -- Les jours depuis la création de la structure
        and DATE_ADD(
            DATE(offerer_creation_date), interval
            offset
            day
        ) <= CURRENT_DATE() -- Que des jours avant aujourd'hui
),

consult_per_offerer_and_day as (
    select
        event_date,
        offerer_id,
        SUM(cnt_events) as nb_daily_consult
    from
        {{ ref('aggregated_daily_offer_consultation_data') }}
    where
        event_name = 'ConsultOffer'
    group by
        1,
        2
),

cum_consult_per_day as (
    select
        *,
        SUM(nb_daily_consult) over (partition by offerer_id order by event_date) as cum_consult
    from
        consult_per_offerer_and_day
)

select
    day as event_date,
    DATE_DIFF(CURRENT_DATE, day, day) as day_seniority,
    all_days_since_activation.offerer_id,
    COALESCE(nb_daily_consult, 0) as nb_daily_consult,
    MAX(cum_consult) over (partition by all_days_since_activation.offerer_id order by day rows unbounded preceding) as cum_consult
from
    all_days_since_activation
    left join
        cum_consult_per_day
        on
            all_days_since_activation.offerer_id = cum_consult_per_day.offerer_id
            and all_days_since_activation.day = cum_consult_per_day.event_date
where
    DATE_DIFF(CURRENT_DATE, day, day) <= 180
