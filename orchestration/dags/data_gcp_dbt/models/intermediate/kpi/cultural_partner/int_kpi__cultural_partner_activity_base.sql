-- Venue-month base for cultural partner activation KPIs, with days since last
-- bookable offer.
with
    -- One row per active venue × month since January 2022
    monthly_partner_activity as (
        select
            gcp.venue_id,
            -- each offset maps to one calendar month since 2022-01-01
            date_trunc(
                date_add(date('2022-01-01'), interval offset month), month
            ) as partition_month
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', month))) as
        offset
        where
            gcp.first_individual_offer_creation_date is not null  -- only venues that created at least one offer
            or gcp.first_collective_offer_creation_date is not null
    ),

    -- Most recent date with a bookable offer per venue per month
    historical_max_dates as (
        select
            m.venue_id,
            m.partition_month,
            -- most recent snapshot date when individual bookable offers existed
            max(
                case
                    when h.total_individual_bookable_offers > 0 then h.partition_date
                end
            ) as last_indiv_date,
            -- most recent snapshot date when collective bookable offers existed
            max(
                case
                    when h.total_collective_bookable_offers > 0 then h.partition_date
                end
            ) as last_collective_date
        from monthly_partner_activity as m
        left join
            {{ ref("int_history__bookable_venue") }} as h
            on m.venue_id = h.venue_id
            and h.partition_date <= last_day(m.partition_month)  -- restrict history to dates within the month
        group by m.venue_id, m.partition_month
    )

select
    bd.venue_id,
    bd.partition_month,
    gcp.partner_region_name,
    gcp.partner_region_code,
    gcp.partner_department_name,
    gcp.partner_department_code,
    gcp.partner_epci_code,
    gcp.partner_city_code,
    gcp.partner_type,
    gcp.offerer_id,
    gvt.venue_tag_name,
    -- days since last individual bookable offer, capped at month-end to handle
    -- current month
    date_diff(
        least(current_date(), last_day(bd.partition_month)), bd.last_indiv_date, day
    ) as days_since_last_indiv_bookable_date,
    -- days since last collective bookable offer, capped at month-end to handle
    -- current month
    date_diff(
        least(current_date(), last_day(bd.partition_month)),
        bd.last_collective_date,
        day
    ) as days_since_last_collective_bookable_date
from historical_max_dates as bd
inner join
    {{ ref("mrt_global__cultural_partner") }} as gcp on bd.venue_id = gcp.venue_id
left join
    {{ ref("mrt_global__venue_tag") }} as gvt
    on gcp.venue_id = gvt.venue_id
    and gvt.venue_tag_category_id = '1'  -- partner counting label tag category
inner join {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
