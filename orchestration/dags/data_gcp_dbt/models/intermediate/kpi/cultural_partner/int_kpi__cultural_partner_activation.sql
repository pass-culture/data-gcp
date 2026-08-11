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
            and gvt.venue_tag_category_id = '1'
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
    ),

    -- First individual offer consultation date per offerer
    offerer_first_consultation as (
        select offerer_id, min(event_date) as offerer_first_consultation_date
        from {{ ref("mrt_native__daily_offer_consultation") }}
        where event_name = 'ConsultOffer' and user_role = 'Bénéficiaire'
        group by offerer_id
    ),

    -- First venue per offerer, with geography and all relevant activation dates
    offerer_first_venue as (
        select
            gcp.offerer_id,
            gcp.partner_city_code,
            gcp.partner_epci_code,
            gcp.partner_department_code,
            gcp.partner_department_name,
            gcp.partner_region_name,
            gcp.partner_region_code,
            gof.offerer_creation_date,
            gof.is_reference_adage,
            date(gof.dms_submitted_at) as first_adage_application_date,
            fc.offerer_first_consultation_date,
            -- null-safe earliest date across bookable and creation dates
            least(
                coalesce(
                    gof.first_individual_bookable_offer_date,
                    gof.first_individual_offer_creation_date
                ),
                coalesce(
                    gof.first_individual_offer_creation_date,
                    gof.first_individual_bookable_offer_date
                )
            ) as first_individual_offer_date,
            gof.first_collective_offer_creation_date
        from {{ ref("mrt_global__cultural_partner") }} as gcp
        inner join {{ ref("mrt_global__offerer") }} as gof using (offerer_id)
        left join offerer_first_consultation as fc using (offerer_id)
        where gof.offerer_creation_date > '2022-01-01'
        qualify
            row_number() over (
                partition by gcp.offerer_id order by gcp.partner_creation_date asc
            )
            = 1  -- keep earliest venue per offerer
    ),

    -- Days from registration to each activation event type per offerer
    time_to_activation as (
        select
            offerer_id,
            partner_city_code,
            partner_epci_code,
            partner_department_code,
            partner_department_name,
            partner_region_name,
            partner_region_code,
            offerer_creation_date,
            -- 0 if already Adage-referenced without a DMS application, else days to
            -- first consultation or Adage DMS
            case
                when is_reference_adage and first_adage_application_date is null
                then 0
                else
                    date_diff(
                        least(
                            coalesce(
                                first_adage_application_date,
                                offerer_first_consultation_date
                            ),
                            coalesce(
                                offerer_first_consultation_date,
                                first_adage_application_date
                            )
                        ),
                        offerer_creation_date,
                        day
                    )
            end as days_to_consultation_or_adage,
            -- 0 if already Adage-referenced without a DMS application, else days to
            -- first individual offer creation or Adage DMS
            case
                when is_reference_adage and first_adage_application_date is null
                then 0
                else
                    date_diff(
                        least(
                            coalesce(
                                first_adage_application_date,
                                first_individual_offer_date
                            ),
                            coalesce(
                                first_individual_offer_date,
                                first_adage_application_date
                            )
                        ),
                        offerer_creation_date,
                        day
                    )
            end as days_to_offer_or_adage,
            date_diff(
                least(
                    coalesce(
                        first_collective_offer_creation_date,
                        first_individual_offer_date
                    ),
                    coalesce(
                        first_individual_offer_date,
                        first_collective_offer_creation_date
                    )
                ),
                offerer_creation_date,
                day
            ) as days_to_any,
            date_diff(
                first_individual_offer_date, offerer_creation_date, day
            ) as days_to_individual,
            date_diff(
                first_collective_offer_creation_date, offerer_creation_date, day
            ) as days_to_collective
        from offerer_first_venue
        where offerer_creation_date <= date_sub(current_date(), interval 30 day)  -- exclude offerers not yet 30 days old
    ),

    -- Offerer activation counts by creation month and first-venue geography
    activation_by_cohort as (
        select
            date_trunc(offerer_creation_date, month) as creation_month,
            partner_city_code,
            partner_epci_code,
            partner_department_code,
            partner_department_name,
            partner_region_name,
            partner_region_code,
            count(distinct offerer_id) as total_offerers_created,
            count(
                distinct case
                    when days_to_consultation_or_adage <= 30 then offerer_id
                end
            ) as total_activated_offerer_consultation_or_adage_30d,
            count(
                distinct case when days_to_offer_or_adage <= 30 then offerer_id end
            ) as total_activated_offerer_offer_or_adage_30d,
            count(
                distinct case when days_to_any <= 30 then offerer_id end
            ) as total_activated_offerer_any_30d,
            count(
                distinct case when days_to_individual <= 30 then offerer_id end
            ) as total_activated_offerer_individual_30d,
            count(
                distinct case when days_to_collective <= 30 then offerer_id end
            ) as total_activated_offerer_collective_30d
        from time_to_activation
        group by
            date_trunc(offerer_creation_date, month),
            partner_city_code,
            partner_epci_code,
            partner_department_code,
            partner_department_name,
            partner_region_name,
            partner_region_code
    )

select
    m.partition_month,
    m.partner_region_name,
    m.partner_region_code,
    m.partner_department_name,
    m.partner_department_code,
    m.partner_epci_code,
    m.partner_city_code,
    m.total_active_partners_individual,
    m.total_active_partners_collective,
    m.total_active_partners_global,
    m.total_active_partners_dual_part,
    m.total_cumulative_activated_partners_individual,
    m.total_cumulative_activated_partners_collective,
    m.total_cumulative_activated_partners_global,
    m.total_cumulative_activated_partners_individual_only,
    m.total_cumulative_activated_partners_collective_only,
    m.total_cumulative_activated_partners_dual_part,
    a.total_offerers_created,
    a.total_activated_offerer_consultation_or_adage_30d,
    a.total_activated_offerer_offer_or_adage_30d,
    a.total_activated_offerer_any_30d,
    a.total_activated_offerer_individual_30d,
    a.total_activated_offerer_collective_30d
from monthly_aggregated_kpis as m
left join
    activation_by_cohort as a
    on m.partition_month = a.creation_month
    and m.partner_city_code = a.partner_city_code
    and m.partner_epci_code = a.partner_epci_code
    and m.partner_department_code = a.partner_department_code
    and m.partner_region_code = a.partner_region_code
where m.partner_city_code is not null
