with
    monthly_partner_activity as (
        select
            gcp.venue_id,
            date_trunc(
                date_add(date('2022-01-01'), interval offset month), month
            ) as partition_month
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', month))) as
        offset
        where
            gcp.first_individual_offer_creation_date is not null
            or gcp.first_collective_offer_creation_date is not null
    ),

    historical_max_dates as (
        select
            m.venue_id,
            m.partition_month,
            max(
                case
                    when h.total_individual_bookable_offers > 0 then h.partition_date
                end
            ) as last_indiv_date,
            max(
                case
                    when h.total_collective_bookable_offers > 0 then h.partition_date
                end
            ) as last_collective_date
        from monthly_partner_activity as m
        left join
            {{ ref("int_history__bookable_venue") }} as h
            on m.venue_id = h.venue_id
            and h.partition_date <= last_day(m.partition_month)
        group by m.venue_id, m.partition_month
    ),

    partner_details as (
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
            date_diff(
                least(current_date(), last_day(bd.partition_month)),
                bd.last_indiv_date,
                day
            ) as days_since_last_indiv_bookable_date,
            date_diff(
                least(current_date(), last_day(bd.partition_month)),
                bd.last_collective_date,
                day
            ) as days_since_last_collective_bookable_date
        from historical_max_dates as bd
        inner join
            {{ ref("mrt_global__cultural_partner") }} as gcp
            on bd.venue_id = gcp.venue_id
        left join
            {{ ref("mrt_global__venue_tag") }} as gvt
            on gcp.venue_id = gvt.venue_id
            and gvt.venue_tag_category_label
            = 'Comptage partenaire label et appellation du MC'
        inner join
            {{ ref("mrt_global__offerer") }} as gof on gcp.offerer_id = gof.offerer_id
    ),

    monthly_aggregated_kpis as (
        select
            partner_region_name,
            partner_region_code,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            partition_month,
            count(
                distinct case
                    when days_since_last_indiv_bookable_date <= 365 then venue_id
                end
            ) as total_active_partners_individual,
            count(
                distinct case
                    when days_since_last_collective_bookable_date <= 365 then venue_id
                end
            ) as total_active_partners_collective,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        or days_since_last_collective_bookable_date <= 365
                    then venue_id
                end
            ) as total_active_partners_global,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date <= 365
                        and days_since_last_collective_bookable_date <= 365
                    then venue_id
                end
            ) as total_active_partners_dual_part,
            count(
                distinct case
                    when days_since_last_indiv_bookable_date >= 0 then venue_id
                end
            ) as total_cumulative_activated_partners_individual,
            count(
                distinct case
                    when days_since_last_collective_bookable_date >= 0 then venue_id
                end
            ) as total_cumulative_activated_partners_collective,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        or days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_activated_partners_global,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date is not null
                        and days_since_last_collective_bookable_date is null
                    then venue_id
                end
            ) as total_cumulative_activated_partners_individual_only,
            count(
                distinct case
                    when
                        days_since_last_collective_bookable_date is not null
                        and days_since_last_indiv_bookable_date is null
                    then venue_id
                end
            ) as total_cumulative_activated_partners_collective_only,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        and days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_activated_partners_dual_part

        from partner_details
        group by
            partition_month,
            partner_region_name,
            partner_region_code,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code
    )

select
    partition_month,
    partner_region_name,
    partner_region_code,
    partner_department_name,
    partner_department_code,
    partner_epci_code,
    partner_city_code,
    total_active_partners_individual,
    total_active_partners_collective,
    total_active_partners_global,
    total_active_partners_dual_part,
    total_cumulative_activated_partners_individual,
    total_cumulative_activated_partners_collective,
    total_cumulative_activated_partners_global,
    total_cumulative_activated_partners_individual_only,
    total_cumulative_activated_partners_collective_only,
    total_cumulative_activated_partners_dual_part
from monthly_aggregated_kpis
where partner_city_code is not null
