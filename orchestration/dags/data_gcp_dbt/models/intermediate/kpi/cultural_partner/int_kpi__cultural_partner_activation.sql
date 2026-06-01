with
    all_activated_partners_and_days as (
        -- Pour chaque venue_id, une ligne par jour depuis la 1ère offre publiée
        select
            gcp.venue_id,
            gcp.first_individual_offer_creation_date,
            gcp.first_collective_offer_creation_date,
            date_add(date('2022-01-01'), interval offset day) as partition_day
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        cross join
            unnest(generate_array(0, date_diff(current_date(), '2022-01-01', day))) as
        offset
        where
            (gcp.first_individual_offer_creation_date is not null)
            or (gcp.first_collective_offer_creation_date is not null)
    ),

    all_days_with_bookability as (
        select
            apd.venue_id,
            apd.first_individual_offer_creation_date,
            apd.first_collective_offer_creation_date,
            apd.partition_day,
            coalesce(
                bvh.total_individual_bookable_offers, 0
            ) as total_indiv_bookable_offers,
            coalesce(
                bvh.total_collective_bookable_offers, 0
            ) as total_collective_bookable_offers
        from all_activated_partners_and_days as apd
        left join
            {{ ref("int_history__bookable_venue") }} as bvh
            on apd.venue_id = bvh.venue_id
            and apd.partition_day = bvh.partition_date
    ),

    bookable_dates as (
        select
            venue_id,
            first_individual_offer_creation_date,
            first_collective_offer_creation_date,
            partition_day,
            date_diff(
                partition_day,
                coalesce(
                    max(
                        case
                            when total_indiv_bookable_offers != 0 then partition_day
                        end
                    ) over (
                        partition by venue_id
                        order by partition_day
                        rows between unbounded preceding and current row
                    ),
                    first_individual_offer_creation_date
                ),
                day
            ) as days_since_last_indiv_bookable_date,
            date_diff(
                partition_day,
                coalesce(
                    max(
                        case
                            when total_collective_bookable_offers != 0
                            then partition_day
                        end
                    ) over (
                        partition by venue_id
                        order by partition_day
                        rows between unbounded preceding and current row
                    ),
                    first_collective_offer_creation_date
                ),
                day
            ) as days_since_last_collective_bookable_date
        from all_days_with_bookability
    ),

    partner_details as (
        select
            bd.venue_id,
            bd.partition_day,
            bd.first_individual_offer_creation_date,
            bd.first_collective_offer_creation_date,
            gcp.partner_region_name,
            rd.region_code as partner_region_code,
            gcp.partner_department_name,
            gcp.partner_department_code,
            gcp.partner_epci_code,
            gcp.partner_city_code,
            gcp.partner_type,
            gcp.offerer_id,
            gvt.venue_tag_name,
            coalesce(
                bd.days_since_last_indiv_bookable_date, -9999
            ) as days_since_last_indiv_bookable_date,
            coalesce(
                bd.days_since_last_collective_bookable_date, -9999
            ) as days_since_last_collective_bookable_date
        from bookable_dates as bd
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
        left join
            {{ ref("region_department") }} as rd
            on gcp.partner_department_code = rd.num_dep
    ),

    daily_aggregated_kpis as (
        select
            partner_region_name,
            partner_region_code,
            partner_department_name,
            partner_department_code,
            partner_epci_code,
            partner_city_code,
            date_trunc(partition_day, month) as partition_month,
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
            ) as total_cumulative_partners_individual,
            count(
                distinct case
                    when days_since_last_collective_bookable_date >= 0 then venue_id
                end
            ) as total_cumulative_partners_collective,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        or days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_partners_global,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        and days_since_last_collective_bookable_date < 0
                    then venue_id
                end
            ) as total_cumulative_partners_individual_only,
            count(
                distinct case
                    when
                        days_since_last_collective_bookable_date >= 0
                        and days_since_last_indiv_bookable_date < 0
                    then venue_id
                end
            ) as total_cumulative_partners_collective_only,
            count(
                distinct case
                    when
                        days_since_last_indiv_bookable_date >= 0
                        and days_since_last_collective_bookable_date >= 0
                    then venue_id
                end
            ) as total_cumulative_partners_dual_part

        from partner_details
        group by
            date_trunc(partition_day, month),
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
    total_cumulative_partners_individual,
    total_cumulative_partners_collective,
    total_cumulative_partners_global,
    total_cumulative_partners_individual_only,
    total_cumulative_partners_collective_only,
    total_cumulative_partners_dual_part
from daily_aggregated_kpis
where partner_city_code is not null
